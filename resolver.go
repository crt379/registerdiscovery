package registerdiscovery

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"
)

const (
	scheme = "etcd"
)

var (
	etcdclient *clientv3.Client
)

func SetEtcdClient(client *clientv3.Client) {
	etcdclient = client
}

func RegisterResolver(client *clientv3.Client) {
	SetEtcdClient(client)
	resolver.Register(&EtcdResolverBuilder{})
}

type EtcdResolverBuilder struct{}

func (*EtcdResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &svcResolver{
		target: target,
		cc:     cc,
	}
	r.start()

	return r, nil
}

func (*EtcdResolverBuilder) Scheme() string {
	return scheme
}

type svcResolver struct {
	target resolver.Target
	cc     resolver.ClientConn
}

func (r *svcResolver) start() {
	logger := zap.L()
	svc := r.target.Endpoint()

	sd := NewServiceDiscovery(etcdclient)
	addrStrs, err := sd.DiscoverServices(svc)
	if err != nil {
		logger.Warn("svcResolver DiscoverServices err", zap.String("error", err.Error()))
	}

	go func() {
		ch := sd.WatchServices(svc)
		for range ch {
			_addrStrs, _err := sd.DiscoverServices(svc)
			if _err != nil {
				logger.Warn("svcResolver DiscoverServices err", zap.String("error", err.Error()))
			}
			_addrs := make([]resolver.Address, len(_addrStrs))
			for i, s := range _addrStrs {
				_addrs[i] = resolver.Address{Addr: s}
			}
			r.cc.UpdateState(resolver.State{Addresses: _addrs})
		}
	}()

	addrs := make([]resolver.Address, len(addrStrs))
	for i, s := range addrStrs {
		addrs[i] = resolver.Address{Addr: s}
	}
	r.cc.UpdateState(resolver.State{Addresses: addrs})
}

func (*svcResolver) ResolveNow(o resolver.ResolveNowOptions) {}

func (*svcResolver) Close() {}
