package collector

import (
	"errors"
	"fmt"
	"github.com/go-kit/log"
	"github.com/tencentyun/tencentcloud-exporter/pkg/common"
	"github.com/tencentyun/tencentcloud-exporter/pkg/metric"
	"strings"
)

// https://cloud.tencent.com/document/product/248/45567
const (
	EmrHBaseNamespace        = "QCE/TXMR_HBASE"
	EmrHBaseInstanceIDKey    = "id4hbaseoverview"
	EmrHBaseMasterHost       = "host4hbaseoverview"
	EmrHBaseRegionServerID   = "id4hbaseregionserver"
	EmrHbaseRegionServerHost = "host4hbaseregionserver"
)

func init() {
	registerHandler(EmrHBaseNamespace, defaultHandlerEnabled, NewEmrHBaseHandler)
}

type emrHBaseHandler struct {
	baseProductHandler
}

func (h *emrHBaseHandler) GetNamespace() string {
	return EmrHBaseNamespace
}

func (h *emrHBaseHandler) GetSeries(m *metric.TcmMetric) ([]*metric.TcmSeries, error) {
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

func NewEmrHBaseHandler(cred common.CredentialIface, c *TcProductCollector, logger log.Logger) (ProductHandler, error) {
	handler := &emrHBaseHandler{
		baseProductHandler{
			monitorQueryKey: EmrHBaseInstanceIDKey,
			collector:       c,
			logger:          logger,
			hMasters:        make(map[string][]string),
			HRegionServers:  make(map[string][]string),
		},
	}
	for _, value := range c.ProductConf.HBasterMasters {
		id, ip, err := parseIDs(value)
		if err != nil {
			logger.Log("msg", "错误的HBase配置", "err", err.Error())
		} else {
			if list, has := handler.hMasters[id]; has {
				list = append(list, ip)
				handler.hMasters[id] = list
			} else {
				var tl []string
				tl = append(tl, ip)
				handler.hMasters[id] = tl
			}
		}
	}
	for _, value := range c.ProductConf.HBaseRegionServers {
		id, ip, err := parseIDs(value)
		if err != nil {
			logger.Log("msg", "错误的HBase配置", "err", err.Error())
		} else {
			if list, has := handler.HRegionServers[id]; has {
				list = append(list, ip)
				handler.HRegionServers[id] = list
			} else {
				var tl []string
				tl = append(tl, ip)
				handler.HRegionServers[id] = tl
			}
		}
	}
	return handler, nil
}

func parseIDs(value string) (string, string, error) {
	list := strings.Split(value, "__")
	if len(list) != 2 {
		return "", "", errors.New(fmt.Sprintf("错误的HBase参数，应该是{EMR 实例 ID}__{节点 IP},现在收到的是: %s", value))
	}
	return list[0], list[1], nil
}
