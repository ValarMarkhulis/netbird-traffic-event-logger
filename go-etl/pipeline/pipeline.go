package pipeline

import (
	"github.com/ValarMarkhulis/traffic-event-logger/go-etl/transformer"
)

func Run(csvFile string, threshold float64) {
	sessions := transformer.ETLPipeline(csvFile, threshold)
	transformer.Load(sessions)
}
