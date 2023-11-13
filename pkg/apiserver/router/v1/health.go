package v1

import (
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/go-chi/chi"
	"net/http"
)

type HealthRouter struct{}

func (hr *HealthRouter) Name() string {
	return "Health"
}

func (hr *HealthRouter) AddRouter(r chi.Router) {
	r.Get("/health", hr.healthCheack)
}

func (hr *HealthRouter) healthCheack(w http.ResponseWriter, r *http.Request) {
	common.Render(w, http.StatusOK, nil)
}
