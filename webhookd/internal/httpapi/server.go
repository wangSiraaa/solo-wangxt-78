// Package httpapi exposes the management REST API (Gin).
package httpapi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/example/webhookd/internal/ssrf"
	"github.com/example/webhookd/internal/store"
	"github.com/example/webhookd/internal/testkit"
)

type Server struct {
	store *store.Store
	guard *ssrf.Guard
	http  *gin.Engine
}

// NewServer builds the router. If tk is non-nil the local test receiver is
// mounted under /testkit (dev/test only).
func NewServer(st *store.Store, guard *ssrf.Guard, tk *testkit.Receiver) *Server {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	s := &Server{store: st, guard: guard, http: r}

	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })

	v1 := r.Group("/v1")
	v1.POST("/endpoints", s.createEndpoint)
	v1.GET("/endpoints", s.listEndpoints)
	v1.GET("/endpoints/:id", s.getEndpoint)
	v1.PATCH("/endpoints/:id", s.updateEndpoint)
	v1.DELETE("/endpoints/:id", s.deleteEndpoint)
	v1.POST("/endpoints/:id/migrate", s.migrateEndpoint)
	v1.POST("/endpoints/:id/rotate-secret", s.rotateSecret)

	v1.POST("/events", s.publishEvent)
	v1.GET("/events/:id", s.getEvent)

	v1.GET("/deliveries", s.listDeliveries)
	v1.GET("/deliveries/:id", s.getDelivery)
	v1.POST("/deliveries/:id/replay", s.replayDelivery)
	v1.POST("/deliveries/:id/skip", s.skipDelivery)

	if tk != nil {
		tk.Mount(r)
	}
	return s
}

func (s *Server) Handler() http.Handler { return s.http }

func abort(c *gin.Context, code int, err error) {
	c.AbortWithStatusJSON(code, gin.H{"error": err.Error()})
}
