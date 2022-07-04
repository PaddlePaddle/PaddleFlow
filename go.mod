module github.com/PaddlePaddle/PaddleFlow

go 1.16

require (
	github.com/BurntSushi/toml v0.4.1 // indirect
	github.com/Microsoft/go-winio v0.5.0 // indirect
	github.com/agiledragon/gomonkey/v2 v2.7.0
	github.com/argoproj/argo v0.0.0-20210119221932-53f022c3f740
	github.com/aws/aws-sdk-go v1.40.25
	github.com/bluele/gcache v0.0.2
	github.com/colinmarc/hdfs/v2 v2.2.0
	github.com/container-storage-interface/spec v1.5.0
	github.com/deckarep/golang-set v1.8.0
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/docker/docker v17.12.1-ce+incompatible
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/emirpasic/gods v1.18.1
	github.com/ghodss/yaml v1.0.0
	github.com/go-chi/chi v1.5.4
	github.com/go-openapi/spec v0.20.4 // indirect
	github.com/go-playground/validator/v10 v10.9.0
	github.com/golang/glog v1.0.0 // indirect
	github.com/google/btree v1.0.0
	github.com/google/go-cmp v0.5.6 // indirect
	github.com/google/uuid v1.3.0
	github.com/gorilla/websocket v1.4.2
	github.com/hanwen/go-fuse/v2 v2.1.0
	github.com/jcmturner/gokrb5/v8 v8.4.2
	github.com/jinzhu/copier v0.3.2
	github.com/kubernetes-csi/csi-lib-utils v0.10.0 // indirect
	github.com/kubernetes-csi/drivers v1.0.2
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-isatty v0.0.13
	github.com/mitchellh/mapstructure v1.4.1
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.0.1 // indirect
	github.com/orcaman/concurrent-map v1.0.0
	github.com/paddleflow/paddle-operator v0.3.1
	github.com/panjf2000/ants/v2 v2.4.8
	github.com/pkg/errors v0.9.1
	github.com/pkg/sftp v1.13.2
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/client_model v0.2.0
	github.com/rifflock/lfshook v0.0.0-20180920164130-b9218ef580f5
	github.com/robfig/cron/v3 v3.0.1
	github.com/shirou/gopsutil/v3 v3.20.10
	github.com/sirupsen/logrus v1.8.1
	github.com/smallnest/chanx v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.1
	github.com/syndtr/goleveldb v1.0.0
	github.com/urfave/cli/v2 v2.4.0
	github.com/vbauerster/mpb/v7 v7.4.1
	github.com/viney-shih/go-lock v1.1.2
	github.com/xujiajun/nutsdb v0.8.0
	go.uber.org/automaxprocs v1.4.0
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97
	golang.org/x/net v0.0.0-20211216030914-fe4d6282115f
	golang.org/x/oauth2 v0.0.0-20211104180415-d3ed0bb246c8 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac // indirect
	golang.org/x/tools v0.1.8 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20211129164237-f09f9a12af12 // indirect
	google.golang.org/grpc v1.42.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.4.0
	gorm.io/driver/mysql v1.1.1
	gorm.io/driver/sqlite v1.1.4
	gorm.io/gorm v1.21.12
	k8s.io/api v0.19.6
	k8s.io/apimachinery v0.19.6
	k8s.io/client-go v0.19.6
	k8s.io/code-generator v0.19.6
	k8s.io/utils v0.0.0-20210707171843-4b05e18ac7d9
	sigs.k8s.io/yaml v1.3.0 // indirect
	volcano.sh/apis v1.3.0-k8s1.18.3-alpha.3
)

replace (
	github.com/container-storage-interface/spec => github.com/container-storage-interface/spec v1.3.0
	github.com/kubernetes-csi/csi-lib-utils => github.com/kubernetes-csi/csi-lib-utils v0.7.0
	golang.org/x/oauth2 => golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	k8s.io/api => k8s.io/api v0.19.6
	k8s.io/apimachinery => k8s.io/apimachinery v0.19.6
	k8s.io/apiserver => k8s.io/apiserver v0.19.6
	k8s.io/client-go => k8s.io/client-go v0.19.6
	k8s.io/code-generator => k8s.io/code-generator v0.19.6
	k8s.io/klog => k8s.io/klog v1.0.0
	volcano.sh/apis => github.com/D0m021ng/apis v1.3.0-k8s1.19.6-21e223
)
