package instance

import (
	"fmt"
	"reflect"

	sdk "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/emr/v20190103"
)

type EmrTcInstance struct {
	baseTcInstance
	meta *sdk.ClusterInstancesInfo
}

func (ins *EmrTcInstance) GetMeta() interface{} {
	return ins.meta
}

func NewEmrTcInstance(instanceId string, meta *sdk.ClusterInstancesInfo) (ins *EmrTcInstance, err error) {
	if instanceId == "" {
		return nil, fmt.Errorf("instanceId is empty ")
	}
	if meta == nil {
		return nil, fmt.Errorf("meta is empty ")
	}
	ins = &EmrTcInstance{
		baseTcInstance: baseTcInstance{
			instanceId: instanceId,
			value:      reflect.ValueOf(*meta),
		},
		meta: meta,
	}
	return
}
