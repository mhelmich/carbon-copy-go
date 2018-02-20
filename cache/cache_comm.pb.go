// Code generated by protoc-gen-go. DO NOT EDIT.
// source: cache_comm.proto

/*
Package cache is a generated protocol buffer package.

It is generated from these files:
	cache_comm.proto

It has these top-level messages:
	Uuid
	GetResponse
	GetsResponse
	GetxResponse
	Get
	Put
	Ack
	Gets
	Puts
	Getx
	Putx
	Inv
	InvAck
	ChangeOwner
	OwnerChanged
*/
package cache

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type OriginalMessageType int32

const (
	OriginalMessageType_GetMessageType  OriginalMessageType = 0
	OriginalMessageType_GetxMessageType OriginalMessageType = 1
)

var OriginalMessageType_name = map[int32]string{
	0: "GetMessageType",
	1: "GetxMessageType",
}
var OriginalMessageType_value = map[string]int32{
	"GetMessageType":  0,
	"GetxMessageType": 1,
}

func (x OriginalMessageType) String() string {
	return proto.EnumName(OriginalMessageType_name, int32(x))
}
func (OriginalMessageType) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

type CacheError int32

const (
	CacheError_NoError CacheError = 0
	CacheError_Timeout CacheError = 1
)

var CacheError_name = map[int32]string{
	0: "NoError",
	1: "Timeout",
}
var CacheError_value = map[string]int32{
	"NoError": 0,
	"Timeout": 1,
}

func (x CacheError) String() string {
	return proto.EnumName(CacheError_name, int32(x))
}
func (CacheError) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

type CacheLineState int32

const (
	CacheLineState_Invalid   CacheLineState = 0
	CacheLineState_Exclusive CacheLineState = 1
	CacheLineState_Shared    CacheLineState = 2
	CacheLineState_Owned     CacheLineState = 3
)

var CacheLineState_name = map[int32]string{
	0: "Invalid",
	1: "Exclusive",
	2: "Shared",
	3: "Owned",
}
var CacheLineState_value = map[string]int32{
	"Invalid":   0,
	"Exclusive": 1,
	"Shared":    2,
	"Owned":     3,
}

func (x CacheLineState) String() string {
	return proto.EnumName(CacheLineState_name, int32(x))
}
func (CacheLineState) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

type Uuid struct {
	Data []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
}

func (m *Uuid) Reset()                    { *m = Uuid{} }
func (m *Uuid) String() string            { return proto.CompactTextString(m) }
func (*Uuid) ProtoMessage()               {}
func (*Uuid) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Uuid) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

type GetResponse struct {
	// Types that are valid to be assigned to InnerMessage:
	//	*GetResponse_Put
	//	*GetResponse_OwnerChanged
	//	*GetResponse_Ack
	InnerMessage isGetResponse_InnerMessage `protobuf_oneof:"innerMessage"`
}

func (m *GetResponse) Reset()                    { *m = GetResponse{} }
func (m *GetResponse) String() string            { return proto.CompactTextString(m) }
func (*GetResponse) ProtoMessage()               {}
func (*GetResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

type isGetResponse_InnerMessage interface {
	isGetResponse_InnerMessage()
}

type GetResponse_Put struct {
	Put *Put `protobuf:"bytes,1,opt,name=put,oneof"`
}
type GetResponse_OwnerChanged struct {
	OwnerChanged *OwnerChanged `protobuf:"bytes,2,opt,name=ownerChanged,oneof"`
}
type GetResponse_Ack struct {
	Ack *Ack `protobuf:"bytes,3,opt,name=ack,oneof"`
}

func (*GetResponse_Put) isGetResponse_InnerMessage()          {}
func (*GetResponse_OwnerChanged) isGetResponse_InnerMessage() {}
func (*GetResponse_Ack) isGetResponse_InnerMessage()          {}

func (m *GetResponse) GetInnerMessage() isGetResponse_InnerMessage {
	if m != nil {
		return m.InnerMessage
	}
	return nil
}

func (m *GetResponse) GetPut() *Put {
	if x, ok := m.GetInnerMessage().(*GetResponse_Put); ok {
		return x.Put
	}
	return nil
}

func (m *GetResponse) GetOwnerChanged() *OwnerChanged {
	if x, ok := m.GetInnerMessage().(*GetResponse_OwnerChanged); ok {
		return x.OwnerChanged
	}
	return nil
}

func (m *GetResponse) GetAck() *Ack {
	if x, ok := m.GetInnerMessage().(*GetResponse_Ack); ok {
		return x.Ack
	}
	return nil
}

// XXX_OneofFuncs is for the internal use of the proto package.
func (*GetResponse) XXX_OneofFuncs() (func(msg proto.Message, b *proto.Buffer) error, func(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error), func(msg proto.Message) (n int), []interface{}) {
	return _GetResponse_OneofMarshaler, _GetResponse_OneofUnmarshaler, _GetResponse_OneofSizer, []interface{}{
		(*GetResponse_Put)(nil),
		(*GetResponse_OwnerChanged)(nil),
		(*GetResponse_Ack)(nil),
	}
}

func _GetResponse_OneofMarshaler(msg proto.Message, b *proto.Buffer) error {
	m := msg.(*GetResponse)
	// innerMessage
	switch x := m.InnerMessage.(type) {
	case *GetResponse_Put:
		b.EncodeVarint(1<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Put); err != nil {
			return err
		}
	case *GetResponse_OwnerChanged:
		b.EncodeVarint(2<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.OwnerChanged); err != nil {
			return err
		}
	case *GetResponse_Ack:
		b.EncodeVarint(3<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Ack); err != nil {
			return err
		}
	case nil:
	default:
		return fmt.Errorf("GetResponse.InnerMessage has unexpected type %T", x)
	}
	return nil
}

func _GetResponse_OneofUnmarshaler(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error) {
	m := msg.(*GetResponse)
	switch tag {
	case 1: // innerMessage.put
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(Put)
		err := b.DecodeMessage(msg)
		m.InnerMessage = &GetResponse_Put{msg}
		return true, err
	case 2: // innerMessage.ownerChanged
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(OwnerChanged)
		err := b.DecodeMessage(msg)
		m.InnerMessage = &GetResponse_OwnerChanged{msg}
		return true, err
	case 3: // innerMessage.ack
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(Ack)
		err := b.DecodeMessage(msg)
		m.InnerMessage = &GetResponse_Ack{msg}
		return true, err
	default:
		return false, nil
	}
}

func _GetResponse_OneofSizer(msg proto.Message) (n int) {
	m := msg.(*GetResponse)
	// innerMessage
	switch x := m.InnerMessage.(type) {
	case *GetResponse_Put:
		s := proto.Size(x.Put)
		n += proto.SizeVarint(1<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *GetResponse_OwnerChanged:
		s := proto.Size(x.OwnerChanged)
		n += proto.SizeVarint(2<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *GetResponse_Ack:
		s := proto.Size(x.Ack)
		n += proto.SizeVarint(3<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case nil:
	default:
		panic(fmt.Sprintf("proto: unexpected type %T in oneof", x))
	}
	return n
}

type GetsResponse struct {
	// Types that are valid to be assigned to InnerMessage:
	//	*GetsResponse_Puts
	//	*GetsResponse_OwnerChanged
	//	*GetsResponse_Ack
	InnerMessage isGetsResponse_InnerMessage `protobuf_oneof:"innerMessage"`
}

func (m *GetsResponse) Reset()                    { *m = GetsResponse{} }
func (m *GetsResponse) String() string            { return proto.CompactTextString(m) }
func (*GetsResponse) ProtoMessage()               {}
func (*GetsResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

type isGetsResponse_InnerMessage interface {
	isGetsResponse_InnerMessage()
}

type GetsResponse_Puts struct {
	Puts *Puts `protobuf:"bytes,1,opt,name=puts,oneof"`
}
type GetsResponse_OwnerChanged struct {
	OwnerChanged *OwnerChanged `protobuf:"bytes,2,opt,name=ownerChanged,oneof"`
}
type GetsResponse_Ack struct {
	Ack *Ack `protobuf:"bytes,3,opt,name=ack,oneof"`
}

func (*GetsResponse_Puts) isGetsResponse_InnerMessage()         {}
func (*GetsResponse_OwnerChanged) isGetsResponse_InnerMessage() {}
func (*GetsResponse_Ack) isGetsResponse_InnerMessage()          {}

func (m *GetsResponse) GetInnerMessage() isGetsResponse_InnerMessage {
	if m != nil {
		return m.InnerMessage
	}
	return nil
}

func (m *GetsResponse) GetPuts() *Puts {
	if x, ok := m.GetInnerMessage().(*GetsResponse_Puts); ok {
		return x.Puts
	}
	return nil
}

func (m *GetsResponse) GetOwnerChanged() *OwnerChanged {
	if x, ok := m.GetInnerMessage().(*GetsResponse_OwnerChanged); ok {
		return x.OwnerChanged
	}
	return nil
}

func (m *GetsResponse) GetAck() *Ack {
	if x, ok := m.GetInnerMessage().(*GetsResponse_Ack); ok {
		return x.Ack
	}
	return nil
}

// XXX_OneofFuncs is for the internal use of the proto package.
func (*GetsResponse) XXX_OneofFuncs() (func(msg proto.Message, b *proto.Buffer) error, func(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error), func(msg proto.Message) (n int), []interface{}) {
	return _GetsResponse_OneofMarshaler, _GetsResponse_OneofUnmarshaler, _GetsResponse_OneofSizer, []interface{}{
		(*GetsResponse_Puts)(nil),
		(*GetsResponse_OwnerChanged)(nil),
		(*GetsResponse_Ack)(nil),
	}
}

func _GetsResponse_OneofMarshaler(msg proto.Message, b *proto.Buffer) error {
	m := msg.(*GetsResponse)
	// innerMessage
	switch x := m.InnerMessage.(type) {
	case *GetsResponse_Puts:
		b.EncodeVarint(1<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Puts); err != nil {
			return err
		}
	case *GetsResponse_OwnerChanged:
		b.EncodeVarint(2<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.OwnerChanged); err != nil {
			return err
		}
	case *GetsResponse_Ack:
		b.EncodeVarint(3<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Ack); err != nil {
			return err
		}
	case nil:
	default:
		return fmt.Errorf("GetsResponse.InnerMessage has unexpected type %T", x)
	}
	return nil
}

func _GetsResponse_OneofUnmarshaler(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error) {
	m := msg.(*GetsResponse)
	switch tag {
	case 1: // innerMessage.puts
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(Puts)
		err := b.DecodeMessage(msg)
		m.InnerMessage = &GetsResponse_Puts{msg}
		return true, err
	case 2: // innerMessage.ownerChanged
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(OwnerChanged)
		err := b.DecodeMessage(msg)
		m.InnerMessage = &GetsResponse_OwnerChanged{msg}
		return true, err
	case 3: // innerMessage.ack
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(Ack)
		err := b.DecodeMessage(msg)
		m.InnerMessage = &GetsResponse_Ack{msg}
		return true, err
	default:
		return false, nil
	}
}

func _GetsResponse_OneofSizer(msg proto.Message) (n int) {
	m := msg.(*GetsResponse)
	// innerMessage
	switch x := m.InnerMessage.(type) {
	case *GetsResponse_Puts:
		s := proto.Size(x.Puts)
		n += proto.SizeVarint(1<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *GetsResponse_OwnerChanged:
		s := proto.Size(x.OwnerChanged)
		n += proto.SizeVarint(2<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *GetsResponse_Ack:
		s := proto.Size(x.Ack)
		n += proto.SizeVarint(3<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case nil:
	default:
		panic(fmt.Sprintf("proto: unexpected type %T in oneof", x))
	}
	return n
}

type GetxResponse struct {
	// Types that are valid to be assigned to InnerMessage:
	//	*GetxResponse_Putx
	//	*GetxResponse_OwnerChanged
	//	*GetxResponse_Ack
	InnerMessage isGetxResponse_InnerMessage `protobuf_oneof:"innerMessage"`
}

func (m *GetxResponse) Reset()                    { *m = GetxResponse{} }
func (m *GetxResponse) String() string            { return proto.CompactTextString(m) }
func (*GetxResponse) ProtoMessage()               {}
func (*GetxResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

type isGetxResponse_InnerMessage interface {
	isGetxResponse_InnerMessage()
}

type GetxResponse_Putx struct {
	Putx *Putx `protobuf:"bytes,1,opt,name=putx,oneof"`
}
type GetxResponse_OwnerChanged struct {
	OwnerChanged *OwnerChanged `protobuf:"bytes,2,opt,name=ownerChanged,oneof"`
}
type GetxResponse_Ack struct {
	Ack *Ack `protobuf:"bytes,3,opt,name=ack,oneof"`
}

func (*GetxResponse_Putx) isGetxResponse_InnerMessage()         {}
func (*GetxResponse_OwnerChanged) isGetxResponse_InnerMessage() {}
func (*GetxResponse_Ack) isGetxResponse_InnerMessage()          {}

func (m *GetxResponse) GetInnerMessage() isGetxResponse_InnerMessage {
	if m != nil {
		return m.InnerMessage
	}
	return nil
}

func (m *GetxResponse) GetPutx() *Putx {
	if x, ok := m.GetInnerMessage().(*GetxResponse_Putx); ok {
		return x.Putx
	}
	return nil
}

func (m *GetxResponse) GetOwnerChanged() *OwnerChanged {
	if x, ok := m.GetInnerMessage().(*GetxResponse_OwnerChanged); ok {
		return x.OwnerChanged
	}
	return nil
}

func (m *GetxResponse) GetAck() *Ack {
	if x, ok := m.GetInnerMessage().(*GetxResponse_Ack); ok {
		return x.Ack
	}
	return nil
}

// XXX_OneofFuncs is for the internal use of the proto package.
func (*GetxResponse) XXX_OneofFuncs() (func(msg proto.Message, b *proto.Buffer) error, func(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error), func(msg proto.Message) (n int), []interface{}) {
	return _GetxResponse_OneofMarshaler, _GetxResponse_OneofUnmarshaler, _GetxResponse_OneofSizer, []interface{}{
		(*GetxResponse_Putx)(nil),
		(*GetxResponse_OwnerChanged)(nil),
		(*GetxResponse_Ack)(nil),
	}
}

func _GetxResponse_OneofMarshaler(msg proto.Message, b *proto.Buffer) error {
	m := msg.(*GetxResponse)
	// innerMessage
	switch x := m.InnerMessage.(type) {
	case *GetxResponse_Putx:
		b.EncodeVarint(1<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Putx); err != nil {
			return err
		}
	case *GetxResponse_OwnerChanged:
		b.EncodeVarint(2<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.OwnerChanged); err != nil {
			return err
		}
	case *GetxResponse_Ack:
		b.EncodeVarint(3<<3 | proto.WireBytes)
		if err := b.EncodeMessage(x.Ack); err != nil {
			return err
		}
	case nil:
	default:
		return fmt.Errorf("GetxResponse.InnerMessage has unexpected type %T", x)
	}
	return nil
}

func _GetxResponse_OneofUnmarshaler(msg proto.Message, tag, wire int, b *proto.Buffer) (bool, error) {
	m := msg.(*GetxResponse)
	switch tag {
	case 1: // innerMessage.putx
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(Putx)
		err := b.DecodeMessage(msg)
		m.InnerMessage = &GetxResponse_Putx{msg}
		return true, err
	case 2: // innerMessage.ownerChanged
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(OwnerChanged)
		err := b.DecodeMessage(msg)
		m.InnerMessage = &GetxResponse_OwnerChanged{msg}
		return true, err
	case 3: // innerMessage.ack
		if wire != proto.WireBytes {
			return true, proto.ErrInternalBadWireType
		}
		msg := new(Ack)
		err := b.DecodeMessage(msg)
		m.InnerMessage = &GetxResponse_Ack{msg}
		return true, err
	default:
		return false, nil
	}
}

func _GetxResponse_OneofSizer(msg proto.Message) (n int) {
	m := msg.(*GetxResponse)
	// innerMessage
	switch x := m.InnerMessage.(type) {
	case *GetxResponse_Putx:
		s := proto.Size(x.Putx)
		n += proto.SizeVarint(1<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *GetxResponse_OwnerChanged:
		s := proto.Size(x.OwnerChanged)
		n += proto.SizeVarint(2<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case *GetxResponse_Ack:
		s := proto.Size(x.Ack)
		n += proto.SizeVarint(3<<3 | proto.WireBytes)
		n += proto.SizeVarint(uint64(s))
		n += s
	case nil:
	default:
		panic(fmt.Sprintf("proto: unexpected type %T in oneof", x))
	}
	return n
}

type Get struct {
	SenderId int32 `protobuf:"varint,1,opt,name=senderId" json:"senderId,omitempty"`
	LineId   int64 `protobuf:"varint,2,opt,name=lineId" json:"lineId,omitempty"`
}

func (m *Get) Reset()                    { *m = Get{} }
func (m *Get) String() string            { return proto.CompactTextString(m) }
func (*Get) ProtoMessage()               {}
func (*Get) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

func (m *Get) GetSenderId() int32 {
	if m != nil {
		return m.SenderId
	}
	return 0
}

func (m *Get) GetLineId() int64 {
	if m != nil {
		return m.LineId
	}
	return 0
}

type Put struct {
	Error    CacheError `protobuf:"varint,1,opt,name=error,enum=cache.CacheError" json:"error,omitempty"`
	SenderId int32      `protobuf:"varint,2,opt,name=senderId" json:"senderId,omitempty"`
	LineId   int64      `protobuf:"varint,3,opt,name=lineId" json:"lineId,omitempty"`
	Version  int32      `protobuf:"varint,4,opt,name=version" json:"version,omitempty"`
	Buffer   []byte     `protobuf:"bytes,5,opt,name=buffer,proto3" json:"buffer,omitempty"`
}

func (m *Put) Reset()                    { *m = Put{} }
func (m *Put) String() string            { return proto.CompactTextString(m) }
func (*Put) ProtoMessage()               {}
func (*Put) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{5} }

func (m *Put) GetError() CacheError {
	if m != nil {
		return m.Error
	}
	return CacheError_NoError
}

func (m *Put) GetSenderId() int32 {
	if m != nil {
		return m.SenderId
	}
	return 0
}

func (m *Put) GetLineId() int64 {
	if m != nil {
		return m.LineId
	}
	return 0
}

func (m *Put) GetVersion() int32 {
	if m != nil {
		return m.Version
	}
	return 0
}

func (m *Put) GetBuffer() []byte {
	if m != nil {
		return m.Buffer
	}
	return nil
}

type Ack struct {
	SenderId int32 `protobuf:"varint,1,opt,name=senderId" json:"senderId,omitempty"`
	LineId   int64 `protobuf:"varint,2,opt,name=lineId" json:"lineId,omitempty"`
}

func (m *Ack) Reset()                    { *m = Ack{} }
func (m *Ack) String() string            { return proto.CompactTextString(m) }
func (*Ack) ProtoMessage()               {}
func (*Ack) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{6} }

func (m *Ack) GetSenderId() int32 {
	if m != nil {
		return m.SenderId
	}
	return 0
}

func (m *Ack) GetLineId() int64 {
	if m != nil {
		return m.LineId
	}
	return 0
}

type Gets struct {
	SenderId int32 `protobuf:"varint,1,opt,name=senderId" json:"senderId,omitempty"`
	LineId   int64 `protobuf:"varint,2,opt,name=lineId" json:"lineId,omitempty"`
}

func (m *Gets) Reset()                    { *m = Gets{} }
func (m *Gets) String() string            { return proto.CompactTextString(m) }
func (*Gets) ProtoMessage()               {}
func (*Gets) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{7} }

func (m *Gets) GetSenderId() int32 {
	if m != nil {
		return m.SenderId
	}
	return 0
}

func (m *Gets) GetLineId() int64 {
	if m != nil {
		return m.LineId
	}
	return 0
}

type Puts struct {
	Error    CacheError `protobuf:"varint,1,opt,name=error,enum=cache.CacheError" json:"error,omitempty"`
	SenderId int32      `protobuf:"varint,2,opt,name=senderId" json:"senderId,omitempty"`
	LineId   int64      `protobuf:"varint,3,opt,name=lineId" json:"lineId,omitempty"`
	Version  int32      `protobuf:"varint,4,opt,name=version" json:"version,omitempty"`
	Sharers  []int32    `protobuf:"varint,5,rep,packed,name=sharers" json:"sharers,omitempty"`
	Buffer   []byte     `protobuf:"bytes,6,opt,name=buffer,proto3" json:"buffer,omitempty"`
}

func (m *Puts) Reset()                    { *m = Puts{} }
func (m *Puts) String() string            { return proto.CompactTextString(m) }
func (*Puts) ProtoMessage()               {}
func (*Puts) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{8} }

func (m *Puts) GetError() CacheError {
	if m != nil {
		return m.Error
	}
	return CacheError_NoError
}

func (m *Puts) GetSenderId() int32 {
	if m != nil {
		return m.SenderId
	}
	return 0
}

func (m *Puts) GetLineId() int64 {
	if m != nil {
		return m.LineId
	}
	return 0
}

func (m *Puts) GetVersion() int32 {
	if m != nil {
		return m.Version
	}
	return 0
}

func (m *Puts) GetSharers() []int32 {
	if m != nil {
		return m.Sharers
	}
	return nil
}

func (m *Puts) GetBuffer() []byte {
	if m != nil {
		return m.Buffer
	}
	return nil
}

type Getx struct {
	SenderId int32 `protobuf:"varint,1,opt,name=senderId" json:"senderId,omitempty"`
	LineId   int64 `protobuf:"varint,2,opt,name=lineId" json:"lineId,omitempty"`
}

func (m *Getx) Reset()                    { *m = Getx{} }
func (m *Getx) String() string            { return proto.CompactTextString(m) }
func (*Getx) ProtoMessage()               {}
func (*Getx) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{9} }

func (m *Getx) GetSenderId() int32 {
	if m != nil {
		return m.SenderId
	}
	return 0
}

func (m *Getx) GetLineId() int64 {
	if m != nil {
		return m.LineId
	}
	return 0
}

type Putx struct {
	Error    CacheError `protobuf:"varint,1,opt,name=error,enum=cache.CacheError" json:"error,omitempty"`
	SenderId int32      `protobuf:"varint,2,opt,name=senderId" json:"senderId,omitempty"`
	LineId   int64      `protobuf:"varint,3,opt,name=lineId" json:"lineId,omitempty"`
	Version  int32      `protobuf:"varint,4,opt,name=version" json:"version,omitempty"`
	Sharers  []int32    `protobuf:"varint,5,rep,packed,name=sharers" json:"sharers,omitempty"`
	Buffer   []byte     `protobuf:"bytes,6,opt,name=buffer,proto3" json:"buffer,omitempty"`
}

func (m *Putx) Reset()                    { *m = Putx{} }
func (m *Putx) String() string            { return proto.CompactTextString(m) }
func (*Putx) ProtoMessage()               {}
func (*Putx) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{10} }

func (m *Putx) GetError() CacheError {
	if m != nil {
		return m.Error
	}
	return CacheError_NoError
}

func (m *Putx) GetSenderId() int32 {
	if m != nil {
		return m.SenderId
	}
	return 0
}

func (m *Putx) GetLineId() int64 {
	if m != nil {
		return m.LineId
	}
	return 0
}

func (m *Putx) GetVersion() int32 {
	if m != nil {
		return m.Version
	}
	return 0
}

func (m *Putx) GetSharers() []int32 {
	if m != nil {
		return m.Sharers
	}
	return nil
}

func (m *Putx) GetBuffer() []byte {
	if m != nil {
		return m.Buffer
	}
	return nil
}

type Inv struct {
	SenderId int32 `protobuf:"varint,1,opt,name=senderId" json:"senderId,omitempty"`
	LineId   int64 `protobuf:"varint,2,opt,name=lineId" json:"lineId,omitempty"`
}

func (m *Inv) Reset()                    { *m = Inv{} }
func (m *Inv) String() string            { return proto.CompactTextString(m) }
func (*Inv) ProtoMessage()               {}
func (*Inv) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{11} }

func (m *Inv) GetSenderId() int32 {
	if m != nil {
		return m.SenderId
	}
	return 0
}

func (m *Inv) GetLineId() int64 {
	if m != nil {
		return m.LineId
	}
	return 0
}

type InvAck struct {
	Error    CacheError `protobuf:"varint,1,opt,name=error,enum=cache.CacheError" json:"error,omitempty"`
	SenderId int32      `protobuf:"varint,2,opt,name=senderId" json:"senderId,omitempty"`
	LineId   int64      `protobuf:"varint,3,opt,name=lineId" json:"lineId,omitempty"`
}

func (m *InvAck) Reset()                    { *m = InvAck{} }
func (m *InvAck) String() string            { return proto.CompactTextString(m) }
func (*InvAck) ProtoMessage()               {}
func (*InvAck) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{12} }

func (m *InvAck) GetError() CacheError {
	if m != nil {
		return m.Error
	}
	return CacheError_NoError
}

func (m *InvAck) GetSenderId() int32 {
	if m != nil {
		return m.SenderId
	}
	return 0
}

func (m *InvAck) GetLineId() int64 {
	if m != nil {
		return m.LineId
	}
	return 0
}

type ChangeOwner struct {
	SenderId int32  `protobuf:"varint,1,opt,name=senderId" json:"senderId,omitempty"`
	LineId   int64  `protobuf:"varint,2,opt,name=lineId" json:"lineId,omitempty"`
	Version  int32  `protobuf:"varint,3,opt,name=version" json:"version,omitempty"`
	Buffer   []byte `protobuf:"bytes,4,opt,name=buffer,proto3" json:"buffer,omitempty"`
}

func (m *ChangeOwner) Reset()                    { *m = ChangeOwner{} }
func (m *ChangeOwner) String() string            { return proto.CompactTextString(m) }
func (*ChangeOwner) ProtoMessage()               {}
func (*ChangeOwner) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{13} }

func (m *ChangeOwner) GetSenderId() int32 {
	if m != nil {
		return m.SenderId
	}
	return 0
}

func (m *ChangeOwner) GetLineId() int64 {
	if m != nil {
		return m.LineId
	}
	return 0
}

func (m *ChangeOwner) GetVersion() int32 {
	if m != nil {
		return m.Version
	}
	return 0
}

func (m *ChangeOwner) GetBuffer() []byte {
	if m != nil {
		return m.Buffer
	}
	return nil
}

type OwnerChanged struct {
	SenderId            int32               `protobuf:"varint,1,opt,name=senderId" json:"senderId,omitempty"`
	LineId              int64               `protobuf:"varint,2,opt,name=lineId" json:"lineId,omitempty"`
	NewOwnerId          int32               `protobuf:"varint,3,opt,name=newOwnerId" json:"newOwnerId,omitempty"`
	OriginalMessageType OriginalMessageType `protobuf:"varint,4,opt,name=originalMessageType,enum=cache.OriginalMessageType" json:"originalMessageType,omitempty"`
}

func (m *OwnerChanged) Reset()                    { *m = OwnerChanged{} }
func (m *OwnerChanged) String() string            { return proto.CompactTextString(m) }
func (*OwnerChanged) ProtoMessage()               {}
func (*OwnerChanged) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{14} }

func (m *OwnerChanged) GetSenderId() int32 {
	if m != nil {
		return m.SenderId
	}
	return 0
}

func (m *OwnerChanged) GetLineId() int64 {
	if m != nil {
		return m.LineId
	}
	return 0
}

func (m *OwnerChanged) GetNewOwnerId() int32 {
	if m != nil {
		return m.NewOwnerId
	}
	return 0
}

func (m *OwnerChanged) GetOriginalMessageType() OriginalMessageType {
	if m != nil {
		return m.OriginalMessageType
	}
	return OriginalMessageType_GetMessageType
}

func init() {
	proto.RegisterType((*Uuid)(nil), "cache.Uuid")
	proto.RegisterType((*GetResponse)(nil), "cache.GetResponse")
	proto.RegisterType((*GetsResponse)(nil), "cache.GetsResponse")
	proto.RegisterType((*GetxResponse)(nil), "cache.GetxResponse")
	proto.RegisterType((*Get)(nil), "cache.Get")
	proto.RegisterType((*Put)(nil), "cache.Put")
	proto.RegisterType((*Ack)(nil), "cache.Ack")
	proto.RegisterType((*Gets)(nil), "cache.Gets")
	proto.RegisterType((*Puts)(nil), "cache.Puts")
	proto.RegisterType((*Getx)(nil), "cache.Getx")
	proto.RegisterType((*Putx)(nil), "cache.Putx")
	proto.RegisterType((*Inv)(nil), "cache.Inv")
	proto.RegisterType((*InvAck)(nil), "cache.InvAck")
	proto.RegisterType((*ChangeOwner)(nil), "cache.ChangeOwner")
	proto.RegisterType((*OwnerChanged)(nil), "cache.OwnerChanged")
	proto.RegisterEnum("cache.OriginalMessageType", OriginalMessageType_name, OriginalMessageType_value)
	proto.RegisterEnum("cache.CacheError", CacheError_name, CacheError_value)
	proto.RegisterEnum("cache.CacheLineState", CacheLineState_name, CacheLineState_value)
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for CacheComm service

type CacheCommClient interface {
	Get(ctx context.Context, in *Get, opts ...grpc.CallOption) (*GetResponse, error)
	Gets(ctx context.Context, in *Gets, opts ...grpc.CallOption) (*GetsResponse, error)
	Getx(ctx context.Context, in *Getx, opts ...grpc.CallOption) (*GetxResponse, error)
	Invalidate(ctx context.Context, in *Inv, opts ...grpc.CallOption) (*InvAck, error)
}

type cacheCommClient struct {
	cc *grpc.ClientConn
}

func NewCacheCommClient(cc *grpc.ClientConn) CacheCommClient {
	return &cacheCommClient{cc}
}

func (c *cacheCommClient) Get(ctx context.Context, in *Get, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := grpc.Invoke(ctx, "/cache.CacheComm/get", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *cacheCommClient) Gets(ctx context.Context, in *Gets, opts ...grpc.CallOption) (*GetsResponse, error) {
	out := new(GetsResponse)
	err := grpc.Invoke(ctx, "/cache.CacheComm/gets", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *cacheCommClient) Getx(ctx context.Context, in *Getx, opts ...grpc.CallOption) (*GetxResponse, error) {
	out := new(GetxResponse)
	err := grpc.Invoke(ctx, "/cache.CacheComm/getx", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *cacheCommClient) Invalidate(ctx context.Context, in *Inv, opts ...grpc.CallOption) (*InvAck, error) {
	out := new(InvAck)
	err := grpc.Invoke(ctx, "/cache.CacheComm/invalidate", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for CacheComm service

type CacheCommServer interface {
	Get(context.Context, *Get) (*GetResponse, error)
	Gets(context.Context, *Gets) (*GetsResponse, error)
	Getx(context.Context, *Getx) (*GetxResponse, error)
	Invalidate(context.Context, *Inv) (*InvAck, error)
}

func RegisterCacheCommServer(s *grpc.Server, srv CacheCommServer) {
	s.RegisterService(&_CacheComm_serviceDesc, srv)
}

func _CacheComm_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Get)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CacheCommServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cache.CacheComm/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CacheCommServer).Get(ctx, req.(*Get))
	}
	return interceptor(ctx, in, info, handler)
}

func _CacheComm_Gets_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Gets)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CacheCommServer).Gets(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cache.CacheComm/Gets",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CacheCommServer).Gets(ctx, req.(*Gets))
	}
	return interceptor(ctx, in, info, handler)
}

func _CacheComm_Getx_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Getx)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CacheCommServer).Getx(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cache.CacheComm/Getx",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CacheCommServer).Getx(ctx, req.(*Getx))
	}
	return interceptor(ctx, in, info, handler)
}

func _CacheComm_Invalidate_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Inv)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CacheCommServer).Invalidate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cache.CacheComm/Invalidate",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CacheCommServer).Invalidate(ctx, req.(*Inv))
	}
	return interceptor(ctx, in, info, handler)
}

var _CacheComm_serviceDesc = grpc.ServiceDesc{
	ServiceName: "cache.CacheComm",
	HandlerType: (*CacheCommServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "get",
			Handler:    _CacheComm_Get_Handler,
		},
		{
			MethodName: "gets",
			Handler:    _CacheComm_Gets_Handler,
		},
		{
			MethodName: "getx",
			Handler:    _CacheComm_Getx_Handler,
		},
		{
			MethodName: "invalidate",
			Handler:    _CacheComm_Invalidate_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "cache_comm.proto",
}

func init() { proto.RegisterFile("cache_comm.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 630 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xd4, 0x56, 0xc1, 0x6e, 0xd3, 0x40,
	0x10, 0xb5, 0x6b, 0x3b, 0xa5, 0xe3, 0x34, 0x98, 0x8d, 0x84, 0x2c, 0x1f, 0xaa, 0xe2, 0x03, 0x85,
	0x1c, 0x7a, 0x08, 0xa7, 0x72, 0x40, 0x2a, 0x51, 0x95, 0x5a, 0x2a, 0x50, 0xb9, 0xe5, 0x8c, 0x5c,
	0x7b, 0x9a, 0x5a, 0x49, 0xd6, 0xd1, 0xee, 0xda, 0x35, 0xff, 0xc1, 0x81, 0x03, 0xff, 0x00, 0x37,
	0x7e, 0x0f, 0xed, 0xc6, 0x69, 0x36, 0xb4, 0xbd, 0x04, 0x41, 0xc5, 0x25, 0xda, 0xd9, 0x79, 0x33,
	0xf3, 0xde, 0x64, 0x76, 0x12, 0xf0, 0xd2, 0x24, 0xbd, 0xc2, 0x4f, 0x69, 0x31, 0x9d, 0xee, 0xcf,
	0x58, 0x21, 0x0a, 0xe2, 0xa8, 0x9b, 0x30, 0x00, 0xfb, 0x63, 0x99, 0x67, 0x84, 0x80, 0x9d, 0x25,
	0x22, 0xf1, 0xcd, 0x5d, 0xf3, 0x45, 0x3b, 0x56, 0xe7, 0xf0, 0xab, 0x09, 0xee, 0x10, 0x45, 0x8c,
	0x7c, 0x56, 0x50, 0x8e, 0x64, 0x07, 0xac, 0x59, 0x29, 0x14, 0xc4, 0xed, 0xc3, 0xbe, 0x4a, 0xb0,
	0x7f, 0x5a, 0x8a, 0x63, 0x23, 0x96, 0x0e, 0x72, 0x00, 0xed, 0xe2, 0x9a, 0x22, 0x1b, 0x5c, 0x25,
	0x74, 0x84, 0x99, 0xbf, 0xa1, 0x80, 0xdd, 0x06, 0xf8, 0x41, 0x73, 0x1d, 0x1b, 0xf1, 0x0a, 0x54,
	0xa6, 0x4e, 0xd2, 0xb1, 0x6f, 0xad, 0xa4, 0x3e, 0x4c, 0xc7, 0x32, 0x75, 0x92, 0x8e, 0xdf, 0x76,
	0xa0, 0x9d, 0x53, 0x8a, 0xec, 0x1d, 0x72, 0x9e, 0x8c, 0x30, 0xfc, 0x66, 0x42, 0x7b, 0x88, 0x82,
	0xdf, 0x70, 0x7b, 0x06, 0xf6, 0xac, 0x14, 0xbc, 0x21, 0xe7, 0x2e, 0xc9, 0xf1, 0x63, 0x23, 0x56,
	0xae, 0x07, 0xa0, 0x57, 0xff, 0x46, 0xaf, 0xbe, 0x4d, 0xaf, 0x6e, 0xe8, 0xd5, 0xff, 0x92, 0xde,
	0x01, 0x58, 0x43, 0x14, 0x24, 0x80, 0x47, 0x1c, 0x69, 0x86, 0x2c, 0xca, 0x14, 0x31, 0x27, 0xbe,
	0xb1, 0xc9, 0x53, 0x68, 0x4d, 0x72, 0x8a, 0xd1, 0x9c, 0x87, 0x15, 0x37, 0x56, 0xf8, 0xc5, 0x04,
	0xeb, 0xb4, 0x14, 0x64, 0x0f, 0x1c, 0x64, 0xac, 0x60, 0x2a, 0xb0, 0xd3, 0x7f, 0xd2, 0x14, 0x1d,
	0xc8, 0xcf, 0x23, 0xe9, 0x88, 0xe7, 0xfe, 0x95, 0x22, 0x1b, 0xf7, 0x16, 0xb1, 0xf4, 0x22, 0xc4,
	0x87, 0xcd, 0x0a, 0x19, 0xcf, 0x0b, 0xea, 0xdb, 0x2a, 0x64, 0x61, 0xca, 0x88, 0x8b, 0xf2, 0xf2,
	0x12, 0x99, 0xef, 0xa8, 0x41, 0x6d, 0x2c, 0xa9, 0xe8, 0x30, 0x1d, 0xaf, 0xa5, 0xe8, 0x35, 0xd8,
	0x72, 0x92, 0xd6, 0x8a, 0xfd, 0x6e, 0x82, 0x2d, 0x67, 0xec, 0xa1, 0xda, 0xe1, 0xc3, 0x26, 0xbf,
	0x4a, 0x18, 0x32, 0xee, 0x3b, 0xbb, 0x96, 0xf4, 0x34, 0xa6, 0xd6, 0xa8, 0xd6, 0x4a, 0xa3, 0xe6,
	0x6a, 0xeb, 0x3f, 0x51, 0x5b, 0xff, 0x3f, 0x6a, 0x0f, 0xc0, 0x8a, 0x68, 0xb5, 0x96, 0x58, 0x84,
	0x56, 0x44, 0x2b, 0x39, 0x54, 0x7f, 0x53, 0x6d, 0xc8, 0xc1, 0x9d, 0xbf, 0x62, 0xf5, 0xc0, 0xd7,
	0x61, 0xaa, 0x37, 0xcc, 0xba, 0xef, 0xb5, 0xd8, 0x2b, 0x6d, 0xf9, 0x61, 0x42, 0x5b, 0x5f, 0x28,
	0x6b, 0x95, 0xdd, 0x01, 0xa0, 0x78, 0xad, 0xd2, 0x34, 0xaa, 0x9c, 0x58, 0xbb, 0x21, 0x27, 0xd0,
	0x2d, 0x58, 0x3e, 0xca, 0x69, 0x32, 0x69, 0xf6, 0xce, 0xf9, 0xe7, 0x19, 0x2a, 0x26, 0x9d, 0x7e,
	0xb0, 0x58, 0x6b, 0xb7, 0x11, 0xf1, 0x5d, 0x61, 0xbd, 0x37, 0xd0, 0xbd, 0x03, 0x4b, 0x08, 0x74,
	0x86, 0x28, 0xb4, 0x1b, 0xcf, 0x20, 0x5d, 0x78, 0x2c, 0x47, 0x5c, 0xbf, 0x34, 0x7b, 0xcf, 0x01,
	0x96, 0x5f, 0x18, 0x71, 0x61, 0xf3, 0x7d, 0xa1, 0x8e, 0x9e, 0x21, 0x8d, 0xf3, 0x7c, 0x8a, 0x45,
	0x29, 0x3c, 0xb3, 0x37, 0x80, 0x8e, 0xc2, 0x9d, 0xe4, 0x14, 0xcf, 0x44, 0x22, 0x50, 0xba, 0x23,
	0x5a, 0x25, 0x93, 0x3c, 0xf3, 0x0c, 0xb2, 0x0d, 0x5b, 0x47, 0x75, 0x3a, 0x29, 0x79, 0x5e, 0xa1,
	0x67, 0x12, 0x80, 0xd6, 0x99, 0x1c, 0xc1, 0xcc, 0xdb, 0x20, 0x5b, 0xe0, 0x48, 0xe9, 0x99, 0x67,
	0xf5, 0x7f, 0x9a, 0xb0, 0xa5, 0xb2, 0x0c, 0x8a, 0xe9, 0x94, 0xec, 0x81, 0x35, 0x42, 0x41, 0x16,
	0x7b, 0x79, 0x88, 0x22, 0x20, 0xcb, 0xf3, 0xe2, 0x27, 0x22, 0x34, 0x48, 0x0f, 0xec, 0x91, 0xdc,
	0x44, 0xee, 0xd2, 0xcb, 0x83, 0xae, 0x66, 0xdc, 0xc2, 0xd6, 0x3a, 0xb6, 0xd6, 0xb1, 0xb5, 0x86,
	0x7d, 0x09, 0x90, 0xcf, 0x15, 0x48, 0x3d, 0x0b, 0x1e, 0x11, 0xad, 0x82, 0xed, 0xe5, 0xf9, 0x30,
	0x1d, 0x87, 0xc6, 0x45, 0x4b, 0xfd, 0x39, 0x78, 0xf5, 0x2b, 0x00, 0x00, 0xff, 0xff, 0xd2, 0xef,
	0xe2, 0xe4, 0x30, 0x08, 0x00, 0x00,
}
