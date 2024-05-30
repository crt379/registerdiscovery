package registerdiscovery

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type ServiceDiscovery struct {
	client        *clientv3.Client
	lock          sync.Mutex
	services      map[string]*map[string]string
	discoverytime map[string]time.Time
	watchtime     map[string]time.Time
}

func NewServiceDiscovery(client *clientv3.Client) *ServiceDiscovery {
	sd := ServiceDiscovery{
		client:        client,
		lock:          sync.Mutex{},
		services:      make(map[string]*map[string]string),
		discoverytime: make(map[string]time.Time),
		watchtime:     make(map[string]time.Time),
	}
	return &sd
}

func (sd *ServiceDiscovery) DiscoverServices(servicename string) ([]string, error) {
	logger := zap.L()

	resp, err := sd.client.Get(context.Background(), ServiceName2Key(servicename), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	sd.lock.Lock()
	defer sd.lock.Unlock()

	var (
		addrs []string
	)

	lasttime, ok := sd.discoverytime[servicename]
	_, iswatch := sd.watchtime[servicename]
	if ok && (iswatch || time.Since(lasttime) < 10*time.Second) {
		addrmap := sd.services[servicename]
		addrs = make([]string, 0, len(*addrmap))
		for _, addr := range *addrmap {
			addrs = append(addrs, addr)
		}

		return addrs, nil
	}

	sd.discoverytime[servicename] = time.Now()

	addrs = make([]string, len(resp.Kvs))
	serviceaddr := make(map[string]string)
	sd.services[servicename] = &serviceaddr
	for i, kv := range resp.Kvs {
		addr := string(kv.Value)
		service := string(kv.Key)
		addrs[i] = string(kv.Value)
		serviceaddr[service] = addr
	}

	logger.Debug(
		"DiscoverServices",
		zap.String("servicename", servicename),
		zap.Any("addrs", addrs),
	)

	return addrs, nil
}

func (sd *ServiceDiscovery) WatchServices(servicename string) <-chan struct{} {
	ch := make(chan struct{}, 100)

	go sd.watchService(servicename, ch)

	return ch
}

func (sd *ServiceDiscovery) watchService(servicename string, notify chan<- struct{}) {
	logger := zap.L()

	sd.lock.Lock()
	sd.watchtime[servicename] = time.Now()
	sd.lock.Unlock()

	dtime, ok := sd.discoverytime[servicename]
	if !ok || time.Since(dtime) > 10*time.Second {
		_, err := sd.DiscoverServices(servicename)
		if err != nil {
			logger.Warn("DiscoverServices err", zap.Error(err))
			return
		}
	}

	watchchen := sd.client.Watch(context.Background(), ServiceName2Key(servicename), clientv3.WithPrefix())
	for watchresp := range watchchen {
		if len(watchresp.Events) == 0 {
			continue
		}
		sd.lock.Lock()
		isnotify := true
		for _, event := range watchresp.Events {
			switch event.Type {
			case clientv3.EventTypePut:
				sd.watchPut(event, servicename)
			case clientv3.EventTypeDelete:
				sd.watchDel(event, servicename)
			default:
				isnotify = false
			}
		}
		if isnotify {
			notify <- struct{}{}
		}
		sd.lock.Unlock()
	}
}

func (sd *ServiceDiscovery) watchPut(event *clientv3.Event, servicename string) {
	logger := zap.L()

	name := string(event.Kv.Key)
	addr := string(event.Kv.Value)
	logger.Debug(fmt.Sprintf("put service: %s -> %s", name, addr))

	m := sd.services[servicename]
	if m == nil {
		sd.services[servicename] = &map[string]string{
			name: addr,
		}
	} else {
		(*m)[name] = addr
	}
}

func (sd *ServiceDiscovery) watchDel(event *clientv3.Event, servicename string) {
	logger := zap.L()

	name := string(event.Kv.Key)
	logger.Debug(fmt.Sprintf("del service: %s", name))

	m := sd.services[servicename]
	if m == nil {
		return
	}
	delete(*m, name)
}
