/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package server

import (
	"github.com/apache/qpid-proton/go/pkg/amqp"
	"github.com/apache/qpid-proton/go/pkg/electron"
	"github.com/lulf/slim/pkg/commitlog"
)

type Server struct {
	container electron.Container
	cl        *commitlog.CommitLog
	codec     *amqp.MessageCodec
}
