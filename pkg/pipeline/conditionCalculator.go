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
	"fmt"

	"github.com/Knetic/govaluate"
)

type ConditionCalculator struct {
	condition string
}

func NewConditionCalculator(condition string) *ConditionCalculator {
	return &ConditionCalculator{
		condition: condition,
	}
}

func (cc *ConditionCalculator) calculate() (bool, error) {
	if cc.condition == "" {
		return true, nil
	}

	expression, err := govaluate.NewEvaluableExpression(cc.condition)
	if err != nil {
		err := fmt.Errorf("cannot calculate the result of condition[%s]: %v", cc.condition, err)
		return false, err
	}

	// 由于 govaluate 库会按照 c 风格的表达式对 condition 进行解析，这样带来结果是：
	// 对于表达式 foo != bar，会将 foo 和 bar 解析成 变量名，而不是字符串，与我们现有场景不符。
	// 因此我们需要对齐做一层转换
	// 相关说明可以参考这里： https://pkg.go.dev/github.com/Knetic/govaluate#section-readme
	tokens := expression.Tokens()
	for i, t := range tokens {
		switch t.Kind {
		case govaluate.VARIABLE:
			t.Kind = govaluate.STRING
		default:
			continue
		}
		tokens[i] = t
	}

	expression, err = govaluate.NewEvaluableExpressionFromTokens(tokens)
	if err != nil {
		err := fmt.Errorf("failed to parse condition[%s]: %v", cc.condition, err)
		return false, err
	}

	result, err := expression.Evaluate(nil)
	if err != nil {
		err := fmt.Errorf("failed to parse condition[%s]: %v", cc.condition, err)
		return false, err
	}

	// result 的类型 依赖于具体的表达式，所以需要在做一次判断
	boolRes, ok := result.(bool)
	if !ok {
		err := fmt.Errorf("the result of condition[%s] cannot trans to bool: %v", cc.condition, boolRes)
		return false, err
	}
	return boolRes, nil
}
