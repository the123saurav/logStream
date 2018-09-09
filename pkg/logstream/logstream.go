package logstream

import (
	"os"
	"log"
	"fmt"
	"encoding/binary"
		"github.com/pkg/errors"
	"bufio"
		)

type logStream struct {
  appendOffset int64 		// where to right next
  startIndex uint64   		// startindex of file
  file *os.File       		// handle to underlying file
  index []int64   // map from index to byte offset
  lastIndex uint64
  lastEntry []byte
}

func (ls logStream)String() string{
	return fmt.Sprintf("\nlogStream: {\n\tstartIndex: %v\n\tlastindex: %v\n\tappendOffset: %v\n\tlines: %v\n\tlastentry: %v}", ls.startIndex, ls.lastIndex, ls.appendOffset, len(ls.index), string(ls.lastEntry))
}

func New(path string) (*logStream, error){
	var startIn uint64
	var lastIn uint64
	var appendOff int64
	var fd *os.File
	lastEntry := []byte{}
	index := []int64{}

	_, err := os.Stat(path)
	if err != nil{
		if os.IsNotExist(err){
			log.Printf("File: %v doesn't exist, creating it", path)
			//TODO check about SYNC flag
			fd, err = os.OpenFile(path, os.O_CREATE|os.O_SYNC|os.O_RDWR, 0664)
			if err != nil {
				return nil, fmt.Errorf("error while creating file: %v", err)
			}
			//Write offset as 0
			b := make([]byte, 8)
			b = append(b, []byte("\n")...)
			binary.BigEndian.PutUint64(b, startIn)
			fd.Write(b)
			return &logStream{appendOffset: appendOff, startIndex: startIn, file:fd, index:index}, nil
		} else {
			return nil, fmt.Errorf("error while opening file: %v", err)
		}
	}
	fd, err = os.OpenFile(path, os.O_SYNC|os.O_RDWR, 0664)
	// Read startIn, populate cache
	b := make([]byte, 9)
	n, err := fd.Read(b)
	if err != nil{
		return nil, fmt.Errorf("error while reading startIndex: %v", err)
	}
	if n != 9{
		return nil, errors.New("Unable to read startIndex")
	}
	startIn = binary.BigEndian.Uint64(b[:8])
	currOffset, _ := fd.Seek(0, 1)

	r1 := bufio.NewReader(fd)
	next := startIn
	for{
		d, err := r1.ReadBytes(10)
		if err != nil{
			break
		}
		lastEntry = d
		index = append(index, currOffset)
		currOffset += int64(len(d))
		//log.Print(string(d))
		next++
	}
	log.Print(index)
	lastIn = next - 1
	currOffset, _ = fd.Seek(0, 1)
	log.Printf("append byte offset: %v", currOffset)
	appendOff = currOffset
	return &logStream{appendOffset: appendOff, startIndex: startIn, file:fd, index:index, lastIndex:lastIn, lastEntry:lastEntry}, nil
}

func (ls *logStream) Append(e []byte) error{
	_, err := ls.file.Write(e)
	if err == nil {
		ls.lastIndex++
		ls.index = append(ls.index, ls.appendOffset)
		currOffset, _ := ls.file.Seek(0, 1)
		log.Printf("append byte offset: %v", currOffset)
		ls.appendOffset = currOffset
		ls.lastEntry = e
	}
	return err
}

func (ls *logStream) GetLastEntry() ([]byte){
	return ls.lastEntry
}

func (ls *logStream) GetLastIndex() uint64{
	return ls.lastIndex
}

func (ls *logStream) GetEntry(in uint64) ([]byte, error){
	if in < ls.startIndex || in > ls.lastIndex{
		return nil, fmt.Errorf("attempt to get out of ranged index: %v", in)
	}
	offset := ls.index[in - ls.startIndex]
	ls.file.Seek(offset, 0)
	r := bufio.NewReader(ls.file)
	d, err := r.ReadBytes(byte(10))
	ls.file.Seek(ls.appendOffset, 0)
	if err != nil{
		return nil, err
	}
	return d, nil
}