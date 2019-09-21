/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package eventserver

import (
	"github.com/lulf/sacl/pkg/eventlog"
	"qpid.apache.org/electron"
)

type EventServer struct {
	container electron.Container
	el        *eventlog.EventLog
}
