package main

import (
    "github.com/the123saurav/logStream/pkg/logstream"
  "log"
)

func main(){
  log.Println("Starting")
  ls, err := logstream.New("/tmp/raft.log")
  if err != nil {
    log.Printf("Error: %v", err)
  } else{
    log.Print(ls)
  }
  //ls.Append([]byte("bye all\n"))
  e, err := ls.GetEntry(5)
  if err != nil{
    log.Printf("Error while getting entry : %v\n%v", 5 , err)
  }
  log.Printf("%v", string(e))
  log.Print(ls)
}
