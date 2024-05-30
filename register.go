package registerdiscovery

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func ServiceName2Key(name string) string {
	return fmt.Sprintf("/%s", name)
}

func nameaddr2key(name, addr string) string {
	return fmt.Sprintf("%s/%s", ServiceName2Key(name), addr)
}

// ctx 可控制租约
func RegisterService(ctx context.Context, service, address string, client *clientv3.Client) error {
	resp, err := client.Grant(context.Background(), 2)
	if err != nil {
		return err
	}

	_, err = client.Put(
		context.Background(),
		nameaddr2key(service, address),
		address,
		clientv3.WithLease(resp.ID),
	)
	if err != nil {
		return err
	}

	// 定期刷新租约
	ch, err := client.KeepAlive(ctx, resp.ID)
	if err != nil {
		return err
	}

	// 监听续约情况
	go func() {
		logger := zap.L()
		for resp := range ch {
			if resp == nil {
				logger.Warn("KeepAlive channel closed")
				return
			}
		}
	}()

	return nil
}
