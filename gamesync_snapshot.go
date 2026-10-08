package main

import (
	"crypto/rand"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	gspb "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
	mmpb "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func textValue(v string) *common.Value {
	return &common.Value{
		ValueType: &common.Value_StringValue{StringValue: v},
	}
}

func intValue(v int64) *common.Value {
	return &common.Value{
		ValueType: &common.Value_IntegerValue{IntegerValue: v},
	}
}

func (s *gamesyncServer) initializeSessionDocuments(proof string) error {
	if s.rooms == nil {
		return nil
	}

	parts := strings.Split(proof, "|")
	session := parts[1]
	cut := strings.LastIndex(session, "/userSessions/")

	if cut < 0 {
		return status.Error(codes.InvalidArgument, "user session resource required")
	}

	roomName := session[:cut]

	var ticket *mmpb.GameSessionCreationTicket

	s.rooms.mu.Lock()

	for _, r := range s.rooms.tickets {
		member := false

		for _, u := range r.ticket.GetGameSession().GetUserSessions() {
			if u.Name == session && u.User == parts[0] {
				member = true
			}
		}

		if member && r.ticket.GetGameSession().GetName() == roomName {
			ticket = proto.Clone(r.ticket).(*mmpb.GameSessionCreationTicket)
			break
		}
	}

	s.rooms.mu.Unlock()

	if ticket == nil {
		return status.Error(codes.NotFound, "assigned room no longer exists")
	}

	s.publication.Lock()
	defer s.publication.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.documents == nil {
		s.documents = map[string]map[string]*gspb.Document{}
	}

	if s.documents[roomName] != nil {
		id := session[cut+len("/userSessions/"):]

		for index, u := range ticket.GameSession.UserSessions {
			if u.Name == session {
				attrs := u.Attributes

				if attrs == nil {
					attrs = &common.MapValue{}
				}

				name := "docs/__us/" + id
				s.documents[roomName][name] = &gspb.Document{
					Name:       name,
					CreateTime: u.CreateTime,
					UpdateTime: u.CreateTime,
					Fields: &common.MapValue{
						Fields: map[string]*common.Value{
							"uid":   textValue(parts[0][strings.LastIndex(parts[0], "/")+1:]),
							"ussid": intValue(int64(index + 1)),
							"ucsid": intValue(int64(index + 1)),
							"st":    intValue(2),
							"tn":    textValue(u.Team),
							"att": {
								ValueType: &common.Value_MapValue{MapValue: attrs},
							},
							"ltc": textValue(gamesyncLatencyJSON(u.LatencyData)),
						},
					},
				}
			}
		}

		s.publishChangesLocked(roomName, s.documents[roomName], map[string]bool{"docs/__us/" + id: true}, ticket.GameSession.CreateTime)

		return nil
	}

	seed := make([]byte, 16)

	if _, err := rand.Read(seed); err != nil {
		return status.Error(codes.Internal, "session entropy unavailable")
	}

	gs := ticket.GameSession
	// Matchmaker participants may request Gamesync in either order. Seed
	// the host from the assigned session, never from the first caller.
	if len(gs.UserSessions) > 0 {
		session = gs.UserSessions[0].Name
		parts[0] = gs.UserSessions[0].User
		cut = strings.LastIndex(session, "/userSessions/")
	}

	gsID := roomName[strings.LastIndex(roomName, "/")+1:]
	usID := session[cut+len("/userSessions/"):]
	ownerID := parts[0][strings.LastIndex(parts[0], "/")+1:]
	config := ticket.MatchmakingConfig

	if v := gs.GetProperties().GetFields()["_BaseConfigName"].GetStringValue(); v != "" {
		config = v
	} else if idx := strings.LastIndex(config, "/"); idx >= 0 {
		config = config[idx+1:]
	}

	fields := map[string]*common.Value{
		"gsid": textValue(gsID),
		"addr": textValue(gs.Host),
		"p":    intValue(int64(gs.Port)),
		"mcn":  textValue(config),
		"maxu": intValue(int64(gs.MaxParticipantCount)),
		"rs": {
			ValueType: &common.Value_BytesValue{BytesValue: seed},
		},
	}

	attrs := ticket.UserDefinitions[0].GetAttributes()

	if attrs == nil {
		attrs = &common.MapValue{
			Fields: map[string]*common.Value{},
		}
	}

	userFields := map[string]*common.Value{
		"uid":   textValue(ownerID),
		"ussid": intValue(1),
		"ucsid": intValue(1),
		"st":    intValue(2),
		"tn":    textValue(ticket.UserDefinitions[0].GetTeam()),
		"att": {
			ValueType: &common.Value_MapValue{MapValue: attrs},
		},
		"ltc": textValue(gamesyncLatencyJSON(ticket.UserDefinitions[0].GetLatencyData())),
	}
	s.documents[roomName] = map[string]*gspb.Document{
		"docs/__gs/f": {
			Name:       "docs/__gs/f",
			Fields:     &common.MapValue{Fields: fields},
			CreateTime: gs.CreateTime,
			UpdateTime: gs.CreateTime,
		},
		"docs/__us/" + usID: {
			Name:       "docs/__us/" + usID,
			Fields:     &common.MapValue{Fields: userFields},
			CreateTime: gs.CreateTime,
			UpdateTime: gs.CreateTime,
		},
	}
	// Jamboree 2.3.0 subscribes to __gs/s and parses it with the mutable
	// session reader at 0xd62214. The unary matchmaking resource is not
	// sufficient to initialize its participation/password/property cache.
	properties := gs.GetProperties()

	if properties == nil {
		properties = &common.MapValue{
			Fields: map[string]*common.Value{},
		}
	}

	s.documents[roomName]["docs/__gs/s"] = &gspb.Document{
		Name:       "docs/__gs/s",
		CreateTime: gs.CreateTime,
		UpdateTime: gs.CreateTime,
		Fields: &common.MapValue{
			Fields: map[string]*common.Value{
				"cp": {
					ValueType: &common.Value_BooleanValue{BooleanValue: gs.CanParticipate},
				},
				"ip": {
					ValueType: &common.Value_BooleanValue{BooleanValue: gs.IsPublic},
				},
				"pw": textValue(gs.Password),
				"ebf": {
					ValueType: &common.Value_BooleanValue{BooleanValue: false},
				},
				"bfmin": intValue(0),
				"bfmax": intValue(0),
				"prp": {
					ValueType: &common.Value_MapValue{
						MapValue: proto.Clone(properties).(*common.MapValue),
					},
				},
			},
		},
	}
	// The host updates the mutable control document with exists=true.
	// __gs/s is the subscribed session state; __gs/m must also exist before
	// the first SetIsPublic/SetSessionEntry operation.
	mutable := proto.Clone(s.documents[roomName]["docs/__gs/s"]).(*gspb.Document)
	mutable.Name = "docs/__gs/m"
	s.documents[roomName][mutable.Name] = mutable

	for index, u := range gs.UserSessions {
		id := u.Name[strings.LastIndex(u.Name, "/")+1:]
		attrs := u.Attributes

		if attrs == nil {
			attrs = &common.MapValue{}
		}

		name := "docs/__us/" + id
		s.documents[roomName][name] = &gspb.Document{
			Name:       name,
			CreateTime: u.CreateTime,
			UpdateTime: u.CreateTime,
			Fields: &common.MapValue{
				Fields: map[string]*common.Value{
					"uid":   textValue(u.User[strings.LastIndex(u.User, "/")+1:]),
					"ussid": intValue(int64(index + 1)),
					"ucsid": intValue(int64(index + 1)),
					"st":    intValue(2),
					"tn":    textValue(u.Team),
					"att": {
						ValueType: &common.Value_MapValue{MapValue: attrs},
					},
					"ltc": textValue(gamesyncLatencyJSON(u.LatencyData)),
				},
			},
		}
	}

	return nil
}

func (s *gamesyncServer) sessionSnapshot(session string, target *gspb.Target) []*gspb.Document {
	cut := strings.LastIndex(session, "/userSessions/")

	if cut < 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	docs := s.documents[session[:cut]]
	out := []*gspb.Document{}

	for name, doc := range docs {
		matches := false

		if t := target.GetDocuments(); t != nil {
			for _, wanted := range t.Documents {
				if wanted == name {
					matches = true
				}
			}
		}

		if c := target.GetCollection(); c != nil {
			prefix := strings.TrimRight(c.Collection, "/") + "/"
			matches = strings.HasPrefix(name, prefix) && !strings.Contains(strings.TrimPrefix(name, prefix), "/")
		}

		if matches {
			out = append(out, proto.Clone(doc).(*gspb.Document))
		}
	}

	return out
}
