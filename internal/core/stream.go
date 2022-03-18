package core

type stream struct {
	msgs []*Message
}

const streamInitialBufSize = 1024

func newStream() *stream {
	return &stream{
		msgs: make([]*Message, 0, streamInitialBufSize),
	}
}

func (s *stream) addMessage(msg *Message) {
	s.msgs = append(s.msgs, msg)
}
