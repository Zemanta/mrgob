package job

import (
	"encoding/json"
	"fmt"
	"os"
)

var ErrMissingJobConfig = fmt.Errorf("Missing job config")

// Config retrieves and decodes the job config passed from the runner.
func Config(target interface{}) error {
	cstr := os.Getenv("mrgob_config")
	if cstr == "" {
		return ErrMissingJobConfig
	}

	return json.Unmarshal([]byte(cstr), target)
}
