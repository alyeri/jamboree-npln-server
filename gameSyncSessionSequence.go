package main

// Call with s.mu held. Session sequence numbers are independent of the
// participant list: removing a guest must never renumber another session.
func (s *gamesyncServer) sessionSequence(room, document string) int64 {
	if existing := s.documents[room][document]; existing != nil {
		if sequence := existing.GetFields().GetFields()["ussid"].GetIntegerValue(); sequence > 0 {
			return sequence
		}
	}

	if s.sessionSequences == nil {
		s.sessionSequences = make(map[string]int64)
	}

	sequence := s.sessionSequences[room]

	for _, existing := range s.documents[room] {
		if value := existing.GetFields().GetFields()["ussid"].GetIntegerValue(); value > sequence {
			sequence = value
		}
	}

	sequence++

	s.sessionSequences[room] = sequence

	return sequence
}
