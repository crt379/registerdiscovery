package registerdiscovery_test

import (
	"context"
	"log"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/crt379/registerdiscovery"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	EtcdClient *clientv3.Client
)

func TestRD(t *testing.T) {
	initetcd()

	logger := zap.NewExample()
	zap.ReplaceGlobals(logger)

	ctx, cancel := context.WithCancel(context.Background())
	registerdiscovery.RegisterService(ctx, "test-test1", "127.0.0.1:12345", EtcdClient)

	ctx1, cancel1 := context.WithCancel(context.Background())
	d := registerdiscovery.NewServiceDiscovery(EtcdClient)
	n := d.WatchServices(ctx1, "test-test1")

	go func() {
		time.Sleep(10 * time.Second)
		logger.Info("c")
		cancel()
	}()

	go func() {
		time.Sleep(20 * time.Second)
		logger.Info("c1")
		cancel1()
	}()

	for range n {
		logger.Debug("nnnn")
	}

	logger.Debug("qqqq")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
}

func initetcd() {
	var err error

	endpoints := []string{
		"192.168.31.112:2379",
	}

	EtcdClient, err = NewEtcdClient(endpoints, 5*time.Second)
	if err != nil {
		log.Panicf(err.Error())
	}
}

func NewEtcdClient(endpoints []string, dialtimeout time.Duration) (*clientv3.Client, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialtimeout,
	})
	if err != nil {
		return client, err
	}

	return client, err
}
