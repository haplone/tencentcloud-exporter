package collector

import (
	"fmt"
	"github.com/go-kit/log"
	"github.com/tencentyun/tencentcloud-exporter/pkg/common"
	"github.com/tencentyun/tencentcloud-exporter/pkg/metric"
)

const (
	EmrHBaseNamespace     = "QCE/TXMR_HBASE"
	EmrHBaseInstanceIDKey = "instanceId"
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

func NewEmrHBaseHandler(cred common.CredentialIface, c *TcProductCollector, logger log.Logger) (handler ProductHandler, err error) {
	handler = &emrHBaseHandler{
		baseProductHandler{
			monitorQueryKey: EmrHBaseInstanceIDKey,
			collector:       c,
			logger:          logger,
		},
	}
	return

}
