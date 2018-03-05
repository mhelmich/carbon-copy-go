// Code generated by protoc-gen-go. DO NOT EDIT.
// source: cluster_comm.proto

package pb

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type NodeInfo struct {
	Addr   string `protobuf:"bytes,1,opt,name=addr" json:"addr,omitempty"`
	NodeId int32  `protobuf:"varint,2,opt,name=nodeId" json:"nodeId,omitempty"`
}

func (m *NodeInfo) Reset()                    { *m = NodeInfo{} }
func (m *NodeInfo) String() string            { return proto.CompactTextString(m) }
func (*NodeInfo) ProtoMessage()               {}
func (*NodeInfo) Descriptor() ([]byte, []int) { return fileDescriptor1, []int{0} }

func (m *NodeInfo) GetAddr() string {
	if m != nil {
		return m.Addr
	}
	return ""
}

func (m *NodeInfo) GetNodeId() int32 {
	if m != nil {
		return m.NodeId
	}
	return 0
}

func init() {
	proto.RegisterType((*NodeInfo)(nil), "pb.NodeInfo")
}

func init() { proto.RegisterFile("cluster_comm.proto", fileDescriptor1) }

var fileDescriptor1 = []byte{
	// 99 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x12, 0x4a, 0xce, 0x29, 0x2d,
	0x2e, 0x49, 0x2d, 0x8a, 0x4f, 0xce, 0xcf, 0xcd, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62,
	0x2a, 0x48, 0x52, 0x32, 0xe3, 0xe2, 0xf0, 0xcb, 0x4f, 0x49, 0xf5, 0xcc, 0x4b, 0xcb, 0x17, 0x12,
	0xe2, 0x62, 0x49, 0x4c, 0x49, 0x29, 0x92, 0x60, 0x54, 0x60, 0xd4, 0xe0, 0x0c, 0x02, 0xb3, 0x85,
	0xc4, 0xb8, 0xd8, 0xf2, 0x40, 0xf2, 0x29, 0x12, 0x4c, 0x0a, 0x8c, 0x1a, 0xac, 0x41, 0x50, 0x5e,
	0x12, 0x1b, 0xd8, 0x08, 0x63, 0x40, 0x00, 0x00, 0x00, 0xff, 0xff, 0x88, 0x37, 0x6e, 0x85, 0x58,
	0x00, 0x00, 0x00,
}