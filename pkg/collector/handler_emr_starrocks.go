package collector

import (
	"fmt"
	"github.com/go-kit/log"
	"github.com/tencentyun/tencentcloud-exporter/pkg/common"
	"github.com/tencentyun/tencentcloud-exporter/pkg/metric"
)

const (
	EmrStarrocksNamespace     = "QCE/TXMR_STARROCKS"
	EmrStarrocksInstanceIDKey = "id4starrocksstarrocksbroker"
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

func NewEmrStarrocksHandler(cred common.CredentialIface, c *TcProductCollector, logger log.Logger) (handler ProductHandler, err error) {
	handler = &emrStarrocksHandler{
		baseProductHandler{
			monitorQueryKey: EmrStarrocksInstanceIDKey,
			collector:       c,
			logger:          logger,
		},
	}
	return

}
