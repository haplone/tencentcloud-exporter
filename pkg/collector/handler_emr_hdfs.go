package collector

import (
	"fmt"
	"github.com/go-kit/log"
	"github.com/tencentyun/tencentcloud-exporter/pkg/common"
	"github.com/tencentyun/tencentcloud-exporter/pkg/metric"
)

const (
	EmrHdfsNamespace     = "QCE/TXMR_HDFS"
	EmrHdfsInstanceIDKey = "id4hdfsoverview"
)

func init() {
	registerHandler(EmrHdfsNamespace, defaultHandlerEnabled, NewEmrHdfsHandler)
}

type emrHdfsHandler struct {
	baseProductHandler
}

func (h *emrHdfsHandler) GetNamespace() string {
	return EmrHdfsNamespace
}

func (h *emrHdfsHandler) GetSeries(m *metric.TcmMetric) ([]*metric.TcmSeries, error) {
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

func NewEmrHdfsHandler(cred common.CredentialIface, c *TcProductCollector, logger log.Logger) (handler ProductHandler, err error) {
	handler = &emrHdfsHandler{
		baseProductHandler{
			monitorQueryKey: EmrHdfsInstanceIDKey,
			collector:       c,
			logger:          logger,
		},
	}
	return

}
