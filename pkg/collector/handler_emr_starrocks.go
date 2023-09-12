package collector

import (
	"github.com/go-kit/log"
	"github.com/tencentyun/tencentcloud-exporter/pkg/common"
)

const (
	EmrStarrocksNamespace     = "QCE/TXMR_STARROCKS"
	EmrStarrocksInstanceIDKey = "instanceId"
)

func init() {
	registerHandler(EmrStarrocksNamespace, defaultHandlerEnabled, NewEmrStarrocksHandler)
}

type emrStarrocksHandler struct {
	baseProductHandler
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
