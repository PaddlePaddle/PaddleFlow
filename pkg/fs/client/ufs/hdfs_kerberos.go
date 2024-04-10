/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ufs

import (
	"encoding/base64"
	"runtime"
	"strings"

	"github.com/colinmarc/hdfs/v2"
	krb "github.com/jcmturner/gokrb5/v8/client"
	krbclient "github.com/jcmturner/gokrb5/v8/client"
	krbconf "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

type KerberosConf struct {
	Realm                  string
	Kdc                    string
	Principal              string
	NameNodePrincipal      string
	DataTransferProtection string
	KeyTabData             string
}

const (
	Krb5ConfTemplate = `[libdefaults]
default_realm = %s
dns_lookup_realm = false
dns_lookup_kdc = false
ticket_lifetime = 72h
renew_lifetime = 7d
forwardable = true
clockskew = 600
[realms]
%s = {
  kdc = %s
  admin_server = %s
}
[domain_realm]
.%s = %s
%s = %s`
)

func buildKerberosConf(properties map[string]interface{}) (*KerberosConf, error) {
	return &KerberosConf{
		Realm:                  properties[common.Realm].(string),
		Kdc:                    properties[common.Kdc].(string),
		Principal:              properties[common.Principal].(string),
		NameNodePrincipal:      properties[common.NameNodePrincipal].(string),
		DataTransferProtection: properties[common.DataTransferProtection].(string),
		KeyTabData:             properties[common.KeyTabData].(string),
	}, nil
}

func NewKerberosClientWithKeyTab(kerberosConf *KerberosConf) (*krb.Client, error) {
	cfg, err := krbconf.NewFromString(Krb5ConfTemplate)
	if err != nil {
		return nil, err
	}
	var kdcs []string
	for _, socket := range strings.Split(kerberosConf.Kdc, ",") {
		ret := strings.Index(socket, ":")
		if ret == -1 {
			kdcs = append(kdcs, socket+":88")
		} else {
			kdcs = append(kdcs, socket)
		}
	}
	cfg.Realms = []krbconf.Realm{
		{
			Realm:         kerberosConf.Realm,
			KDC:           kdcs,
			DefaultDomain: kerberosConf.Realm,
		},
	}

	cfg.LibDefaults.DefaultRealm = kerberosConf.Realm

	domainRealm := make(krbconf.DomainRealm)
	lowerRealm := strings.ToLower(kerberosConf.Realm)
	domainRealm[lowerRealm] = kerberosConf.Realm
	domainRealm["."+lowerRealm] = kerberosConf.Realm
	cfg.DomainRealm = domainRealm

	keyTabData, err := base64.StdEncoding.DecodeString(kerberosConf.KeyTabData)
	if err != nil {
		return nil, err
	}

	kt := new(keytab.Keytab)
	if err := kt.Unmarshal(keyTabData); err != nil {
		return nil, err
	}

	principleName := strings.Split(kerberosConf.Principal, "@")[0]
	kerberosClient := krbclient.NewWithKeytab(principleName, cfg.LibDefaults.DefaultRealm, kt, cfg)
	err = kerberosClient.Login()
	if err != nil {
		return nil, err
	}
	return kerberosClient, nil
}

func NewHdfsWithKerberosFileSystem(properties map[string]interface{}) (UnderFileStorage, error) {
	nameNodeAddress := properties[common.NameNodeAddress].(string)
	options := hdfs.ClientOptions{
		Addresses: strings.Split(nameNodeAddress, ","),
	}
	useDatanodeHostname, ok := properties[common.UseDatanodeHostname].(bool)
	if ok && useDatanodeHostname {
		options.UseDatanodeHostname = true
	}
	krbConfig, _ := buildKerberosConf(properties)
	if krbClient, err := NewKerberosClientWithKeyTab(krbConfig); err == nil {
		options.KerberosClient = krbClient
		options.KerberosServicePrincipleName = strings.Split(krbConfig.NameNodePrincipal, "@")[0]
		options.DataTransferProtection = krbConfig.DataTransferProtection
		//if options.DataTransferProtection == "" {
		//	options.DataTransferProtection = "integrity"
		//}
	} else {
		return nil, err
	}

	cli, err := hdfs.NewClient(options)
	if err != nil {
		return nil, err
	}

	subpath, ok := properties[common.SubPath]
	if !ok {
		subpath = "/"
	} else {
		// create empty subpath
		if err := cli.MkdirAll(subpath.(string), 0644); err != nil {
			return nil, err
		}
	}

	blockSize, ok := properties[common.BlockSizeKey].(int64)
	if !ok {
		blockSize = DefaultBlockSize
	}
	replication, ok := properties[common.ReplicationKey].(int)
	if !ok {
		replication = DefaultReplication
	}

	fs := &hdfsFileSystem{
		client:      cli,
		blockSize:   blockSize,
		replication: replication,
		subpath:     subpath.(string),
	}
	runtime.SetFinalizer(fs, func(fs *hdfsFileSystem) {
		fs.client.Close()
	})
	return fs, nil
}

// HDFS with Kerberos
func init() {
	RegisterUFS(common.HDFSWithKerberosType, NewHdfsWithKerberosFileSystem)
}
