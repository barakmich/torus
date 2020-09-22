module github.com/coreos/torus

go 1.15

require (
	bazil.org/fuse v0.0.0-20200524192727-fb710f7dfd05
	github.com/DeanThompson/ginpprof v0.0.0-20151223020339-18e555cdf1a9
	github.com/RoaringBitmap/roaring v0.2.5
	github.com/Sirupsen/logrus v0.10.1-0.20160829202321-3ec0642a7fb6
	github.com/barakmich/mmap-go v0.0.0-20160510231039-c4bd255520e5
	github.com/beorn7/perks v0.0.0-20160229213445-3ac7bf7a47d1
	github.com/cloudfoundry-incubator/candiedyaml v0.0.0-20160429080125-99c3df83b515
	github.com/coreos/etcd v3.3.25+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20160527140244-4484981625c1
	github.com/coreos/go-tcmu v0.0.0-20160831184627-a6fc46a3b7d2
	github.com/coreos/pkg v0.0.0-20160530111557-7f080b6c11ac
	github.com/dustin/go-humanize v0.0.0-20160601001646-88e58c26e9fe
	github.com/ghodss/yaml v0.0.0-20160503190739-e8e0db901617
	github.com/gin-gonic/gin v0.0.0-20160525124545-f931d1ea80ae
	github.com/godbus/dbus v4.0.1-0.20160506222550-32c6cc29c145+incompatible
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.3
	github.com/google/uuid v1.1.2 // indirect
	github.com/inconshreveable/mousetrap v1.0.0
	github.com/kardianos/osext v0.0.0-20151222153229-29ae4ffbc9a6
	github.com/lpabon/godbc v0.1.2-0.20140613165803-9577782540c1
	github.com/manucorporat/sse v0.0.0-20160126180136-ee05b128a739
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/mattn/goreman v0.3.7 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1
	github.com/mdlayher/aoe v0.0.0-20161107164028-f389bbabc496
	github.com/mdlayher/ethernet v0.0.0-20150827222903-28018267bba4
	github.com/mdlayher/raw v0.0.0-20160616223208-b730b008e228
	github.com/pborman/uuid v0.0.0-20160216163710-c55201b03606
	github.com/prometheus/client_golang v0.0.0-20160531091528-488edd04dc22
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4
	github.com/prometheus/common v0.0.0-20160530185023-a3a8fe85f257
	github.com/prometheus/procfs v0.0.0-20160411190841-abf152e5f3e9
	github.com/ricochet2200/go-disk-usage v0.0.0-20150921141558-f0d1b743428f
	github.com/serialx/hashring v0.0.0-20160507062712-75d57fa264ad
	github.com/spf13/cobra v0.0.0-20160517171929-f36824430130
	github.com/spf13/pflag v0.0.0-20160427162146-cb88ea77998c
	go.uber.org/zap v1.16.0 // indirect
	golang.org/x/crypto v0.0.0-20191011191535-87dc89f01550
	golang.org/x/net v0.0.0-20200226121028-0de0cce0169b
	golang.org/x/sys v0.0.0-20200918174421-af09f7315aff
	golang.org/x/time v0.0.0-20160202183820-a4bde1265759
	google.golang.org/grpc v1.29.1
	gopkg.in/go-playground/validator.v8 v8.15.1
	gopkg.in/yaml.v2 v2.3.0
)

replace google.golang.org/grpc => google.golang.org/grpc v1.26.0
