package object

import (
	"github.com/baidubce/bce-sdk-go/services/sts"
	"github.com/baidubce/bce-sdk-go/services/sts/api"
	log "github.com/sirupsen/logrus"
)

func StsSessionToken(ak string, sk string, duration int, acl string) (*api.GetSessionTokenResult, error) {
	stsClient, err := sts.NewClient(ak, sk)
	if err != nil {
		log.Errorf("create sts client object: %v", err)
		return nil, err
	}

	result, err := stsClient.GetSessionToken(duration, acl)
	if err != nil {
		log.Errorf("get session token failed: %v", err)
		return nil, err
	}
	return result, nil
}
