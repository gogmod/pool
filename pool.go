package pool

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
)

type (
	//资源池[设置最大容量]
	Pool interface {
		//调用资源池中的资源
		Call(func(Src) error) error
		//销毁资源池
		Release()
		//返回当前资源池中资源数量
		Len() int
	}
	//经典资源池
	classic struct {
		srcs     chan Src      //资源列表(Src必须为指针类型)
		capacity int           //资源池容量
		maxIdle  int           //资源最大空闲数
		len      int           //资源数量
		factory  Factory       //创建资源的方法
		gctime   time.Duration //空闲资源回收时间
		released bool          //标记是否已经关闭资源池
		sync.RWMutex
	}
	//资源接口
	Src interface {
		//判断资源是否可用
		IsUsable() bool
		//使用后的重置方法
		Reset()
		//被资源池释放前的自毁方法
		Release()
	}
	//创建资源的方法
	Factory func() (Src, error)
)

const GC_TIME = 60e9

var releasedError = errors.New("资源池已关闭")

//ClassicPool... 构建经典资源池
func ClassicPool(capacity, maxIdle int, factory Factory, gctime ...time.Duration) Pool {
	if len(gctime) == 0 {
		gctime = append(gctime, GC_TIME)
	}
	pool := &classic{
		srcs:     make(chan Src, capacity),
		capacity: capacity,
		maxIdle:  maxIdle,
		factory:  factory,
		gctime:   gctime[0],
		released: false,
	}
	go pool.gc()
	return pool
}

//调用资源池中的资源
func (self *classic) Call(callback func(Src) error) (err error) {
	var src Src
wait:
	self.RLock()
	if self.released {
		self.RUnlock()
		return releasedError
	}
	select {
	case src = <-self.srcs:
		self.RUnlock()
		if !src.IsUsable() {
			self.del(src)
			goto wait
		}
	default:
		self.RUnlock()
		err = self.incAuto()
		if err != nil {
			return err
		}
		runtime.Gosched()
		goto wait
	}
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%v", p)
		}
		self.recover(src)
	}()
	err = callback(src)
	return err
}

//销毁资源池
func (self *classic) Release() {
	self.Lock()
	defer self.Unlock()
	if self.released {
		return
	}
	self.released = true
	for i := len(self.srcs); i >= 0; i-- {
		(<-self.srcs).Release()
	}
	close(self.srcs)
	self.len = 0
}

//返回当前资源池剩余的数量
func (self *classic) Len() int {
	self.RLock()
	defer self.RUnlock()
	return self.len
}

// 空闲资源回收协程
func (self *classic) gc() {
	for !self.isReleased() {
		self.Lock()
		extra := len(self.srcs) - self.maxIdle
		if extra > 0 {
			self.len -= extra
			for ; extra > 0; extra-- {
				(<-self.srcs).Release()
			}
		}
		self.Unlock()
		time.Sleep(self.gctime)
	}
}

//资源扩容
func (self *classic) incAuto() error {
	self.Lock()
	defer self.Unlock()
	if self.len >= self.capacity {
		return nil
	}
	src, err := self.factory()
	if err != nil {
		return err
	}
	self.srcs <- src
	self.len++
	return nil
}

//删除资源
func (self *classic) del(src Src) {
	src.Release()
	self.Lock()
	self.len--
	self.Unlock()
}

//恢复/重置
func (self *classic) recover(src Src) {
	self.RLock()
	defer self.RUnlock()
	if self.released {
		return
	}
	src.Reset()
	self.srcs <- src
}

//资源是否被释放
func (self *classic) isReleased() bool {
	self.RLock()
	defer self.RUnlock()
	return self.released
}
