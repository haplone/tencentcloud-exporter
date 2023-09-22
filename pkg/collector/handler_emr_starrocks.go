package collector

import (
	"errors"
	"fmt"
	"github.com/go-kit/log"
	"github.com/tencentyun/tencentcloud-exporter/pkg/common"
	"github.com/tencentyun/tencentcloud-exporter/pkg/metric"
	"strings"
)

const (
	EmrStarrocksNamespace     = "QCE/TXMR_STARROCKS"
	EmrStarrocksInstanceIDKey = "instanceId"
	EmrStarrocksBeHost        = "host4starrocksstarrocksbe"
	EmrStarrocksBeID          = "id4starrocksstarrocksbe"
	EmrStarrocksBrokerHost    = "host4starrocksstarrocksbroker"
	EmrStarrocksBrokerID      = "id4starrocksstarrocksbroker"
	EmrStarrocksFeHost        = "host4starrocksstarrocksfe"
	EmrStarrocksFeID          = "id4starrocksstarrocksfe"
)

func init() {
	registerHandler(EmrStarrocksNamespace, defaultHandlerEnabled, NewEmrStarrocksHandler)
}

type emrStarrocksHandler struct {
	baseProductHandler
}

func (h *emrStarrocksHandler) GetSeries(m *metric.TcmMetric) ([]*metric.TcmSeries, error) {
	fmt.Println("GetSeriess")
	fmt.Printf("%v\r\n", m)
	if m.Conf.IsIncludeOnlyInstance() {
		return h.GetSeriesByOnly(m)
	}

	if m.Conf.IsIncludeAllInstance() {
		return h.GetSeriesByAll(m)
	}

	if m.Conf.IsCustomQueryDimensions() {
		return h.GetSeriesByCustom(m)
	}

	return nil, fmt.Errorf("must config all_instances or only_include_instances or custom_query_dimensions")
}

func (h *emrStarrocksHandler) GetNamespace() string {
	return EmrStarrocksNamespace
}

func NewEmrStarrocksHandler(cred common.CredentialIface, c *TcProductCollector, logger log.Logger) (ProductHandler, error) {
	handler := &emrStarrocksHandler{
		baseProductHandler{
			monitorQueryKey:  EmrStarrocksInstanceIDKey,
			collector:        c,
			logger:           logger,
			starrocksBEs:     make(map[string][]string),
			starrocksFEs:     make(map[string][]string),
			starrocksBrokers: make(map[string][]string),
		},
	}
	for _, value := range c.ProductConf.StarrocksBEs {
		id, ip, err := parseStarrocksIDs(value)
		if err != nil {
			logger.Log("msg", "错误的Starrocks配置", "err", err.Error())
		} else {
			if list, has := handler.starrocksBEs[id]; has {
				list = append(list, ip)
				handler.starrocksBEs[id] = list
			} else {
				var tl []string
				tl = append(tl, ip)
				handler.starrocksBEs[id] = tl
			}
		}
	}
	for _, value := range c.ProductConf.StarrocksFEs {
		id, ip, err := parseStarrocksIDs(value)
		if err != nil {
			logger.Log("msg", "错误的Starrocks配置", "err", err.Error())
		} else {
			if list, has := handler.starrocksFEs[id]; has {
				list = append(list, ip)
				handler.starrocksFEs[id] = list
			} else {
				var tl []string
				tl = append(tl, ip)
				handler.starrocksFEs[id] = tl
			}

		}
	}
	for _, value := range c.ProductConf.StarrocksBrokers {
		id, ip, err := parseStarrocksIDs(value)
		if err != nil {
			logger.Log("msg", "错误的Starrocks配置", "err", err.Error())
		} else {
			if list, has := handler.starrocksBrokers[id]; has {
				list = append(list, ip)
				handler.starrocksBrokers[id] = list
			} else {
				var tl []string
				tl = append(tl, ip)
				handler.starrocksBrokers[id] = tl
			}

		}
	}
	return handler, nil

}

func parseStarrocksIDs(value string) (string, string, error) {
	list := strings.Split(value, "__")
	if len(list) != 2 {
		return "", "", errors.New(fmt.Sprintf("错误的starrocks参数，应该是{EMR 实例 ID}__{节点 IP},现在收到的是: %s", value))
	}
	return list[0], list[1], nil
}
