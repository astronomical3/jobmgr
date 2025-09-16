package internal

import ()

// Constructor function for creating a new instance of the process choices map.
func NewProcessChoices(j *JobHandler) map[string]func() {
	return map[string]func() {
		// This key-value pair is used to reference how to register a custom
		//   process of the JobHandler.
		// To disable use of the j.countTo10 sample process, comment this pair
		//   out when registering your own processes here.
		"countTo10": j.countTo10,
	}
}