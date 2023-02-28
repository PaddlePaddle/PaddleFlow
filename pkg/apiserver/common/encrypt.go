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

package common

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
)

const (
	AESEncryptKey    = "paddleflow123456" // 长度必须为16，分别对应加密算法AES-128
	AESEncryptKeyEnv = "AESEncryptKey"
	PFSecretName     = "pf-secret"
)

func EncryptPk(pk int64) (string, error) {
	return AesEncrypt(strconv.FormatInt(pk, 10), GetAESEncryptKey())
}

func GetAESEncryptKey() string {
	aesEncryptKey := os.Getenv(AESEncryptKeyEnv)
	if aesEncryptKey != "" {
		return aesEncryptKey
	}
	return AESEncryptKey
}

func DecryptPk(data string) (int64, error) {
	if data == "" {
		return 0, fmt.Errorf("DecryptPk data is null")
	}
	plainText, err := AesDecrypt(data, GetAESEncryptKey())
	if err != nil {
		log.Errorf("AesDecrypt data failed. data:[%s]", data)
		return 0, err
	}
	log.Debugf("AesDecrypt data succeed. data:[%s] plainText:[%s]", data, plainText)
	pk, err := strconv.ParseInt(plainText, 10, 64)
	if err != nil {
		log.Errorf("DecryptPk parse int error. plainText:[%s] error:[%s]", plainText, err.Error())
		return 0, err
	}
	return pk, nil
}

func AesEncrypt(orig string, key string) (string, error) {
	if orig == "" {
		return "", fmt.Errorf("AesEncrypt orig is null")
	}
	origData := []byte(orig)
	k := []byte(key)
	block, _ := aes.NewCipher(k)
	blockSize := block.BlockSize()
	origData = PKCS7Padding(origData, blockSize)
	blockMode := cipher.NewCBCEncrypter(block, k[:blockSize])
	encrypted := make([]byte, len(origData))
	blockMode.CryptBlocks(encrypted, origData)
	log.Debugf("AesEncrypt encode string:%s", hex.EncodeToString(encrypted))
	return hex.EncodeToString(encrypted), nil
}

func AesDecrypt(encrypted string, key string) (string, error) {
	encryptedByte, err := hex.DecodeString(encrypted)
	if err != nil {
		log.Errorf("AesDecrypt decode string error. string:[%s] error:[%s]",
			encrypted, err.Error())
		return "", err
	}
	k := []byte(key)
	block, _ := aes.NewCipher(k)
	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, k[:blockSize])
	orig := make([]byte, len(encryptedByte))
	blockMode.CryptBlocks(orig, encryptedByte)
	orig, err = PKCS7UnPadding(orig)
	if err != nil {
		return "", err
	}
	return string(orig), nil
}

func PKCS7Padding(ciphertext []byte, blocksize int) []byte {
	padding := blocksize - len(ciphertext)%blocksize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS7UnPadding(origData []byte) ([]byte, error) {
	length := len(origData)
	unpadding := int(origData[length-1])
	if length < unpadding {
		return nil, fmt.Errorf("AesDecrypt PKCS7UnPadding failed as input data illegal")
	}
	return origData[:(length - unpadding)], nil
}

func GetMD5Hash(content []byte) string {
	hash := md5.Sum(content)
	return hex.EncodeToString(hash[:])
}
