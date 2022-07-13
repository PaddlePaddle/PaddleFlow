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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCaculatCondition(t *testing.T) {
	cc := NewConditionCalculator("10 > 11")
	result, err := cc.calculate()

	assert.Nil(t, err)
	assert.False(t, result)

	cc = NewConditionCalculator("abc > def")
	result, err = cc.calculate()

	assert.Nil(t, err)
	assert.False(t, result)

	cc = NewConditionCalculator("abc < def")
	result, err = cc.calculate()
	assert.True(t, result)

	cc = NewConditionCalculator("9 < 10")
	result, err = cc.calculate()
	assert.True(t, result)

	cc = NewConditionCalculator("'9' > '10'")
	result, err = cc.calculate()
	assert.True(t, result)

}
