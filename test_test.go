package learning

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestAccessNonExistentElementOfMap(t *testing.T) {
	m1 := make(map[int]int)
	if m1[2] != 0 {
		t.Fatal()
	}
	m2 := make(map[string][]string)
	if m2["abc"] != nil {
		t.Fatal()
	}
	if _, ok := m2["abc"]; ok {
		t.Fatal()
	}
}

type testInitialValue struct {
	s string
}

func TestInitialValue(t *testing.T) {
	x := testInitialValue{} // string成员的初值是空字符串""，而不是nil。
	if x.s != "" {
		t.Fatal()
	}
}

func Test1(t *testing.T) {
	ch := make(chan bool)
	go func() {
		select {
		case <-ch:
			fmt.Println("ds")
		default:
			fmt.Println("default")
		}
	}()
	// ch<-false和close(ch)都可以触发case<-ch，无论有无default，只不过如果有default，select会在没有任何case触发的情况下执行default。
	// ch <- false
	close(ch)
	// 对一个已经被close过的channel进行接收操作依然可以接受到之前已经成功发送的数据；如果channel中已经没有数据的话将产生一个零值的数据。
	// 如果有多个线程阻塞在ch上，且我们要让这些线程都退出，那么可以close(ch)，或者在ch上发送足够多的值使这些线程退出。

	// 注意让主线程等待子线程执行完，如果主线程不等待，将会很快退出，子线程也将退出，如果子线程没有被调度执行，将会使我们观察到非预期的、
	// 错误的结果。更严谨可以使用sync.WaitGroup。
	time.Sleep(2 * time.Second)
}

func Test2(t *testing.T) {
	// 不要因为受C的switch影响，go的switch每一个case默认都有一个break，除非显式指定fallthrough，
	// 并且只会继续执行下一个且仅一个case的代码。
	// 另外，不要因为select与switch相似的语法犯同样的错误，select中多个case不能共用一个代码块。
	i := 1
	switch i {
	case 1:
		fallthrough
	case 0:
		fmt.Println("switch_0!")
	case 3:
		fmt.Println("switch_3!")
	}
	// tick := time.Tick(100 * time.Millisecond)
	// boom := time.After(500 * time.Millisecond)
	// for {
	// 	select {
	//     case <-tick:
	//     case <-boom:
	// 		fmt.Println("select!")
	//     // XXX 只会出现一次select
	// 	}
	// }
}

type AppendEntriesArgs struct {
	Entries []int
}

func Test5(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fmt.Println(i)
		}() // 传同一个对象引用
	}
	wg.Wait()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			fmt.Println(k)
		}(i) // 传值
	}
	wg.Wait()
	for i := 0; i < 3; i++ {
		args := AppendEntriesArgs{Entries: nil}
		args.Entries = make([]int, i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(100)
			fmt.Println(len(args.Entries))
		}() // 传引用，且引用的是不同的对象，而不是同一个对象
	}
	wg.Wait()
}

func Test13(t *testing.T) {
	// ticker创建时不会马上触发一次，而是过一段时间触发一次。
	ticker := time.NewTicker(2 * time.Second)
	t1 := time.Now().Unix()
	select {
	case <-ticker.C:
		t2 := time.Now().Unix()
		fmt.Printf("%d\n", t2-t1) // 2
	}
}

func Test14(t *testing.T) {
	// 如果select监听的channel已被关闭，会进入这个channel对应的块。
	// 如果select监听该channel之前，该channel已被关闭，那么select会马上执行对应的块，不会阻塞。
	ch := make(chan struct{})
	go func() {
		time.Sleep(2 * time.Second)
		select {
		case <-ch:
			fmt.Println("goroutine exit")
		}
	}()
	close(ch)
	fmt.Println("close ch")
	time.Sleep(3 * time.Second)
}

type TestGobT struct {
	x int
	y string
	z bool
}

func TestGob(t *testing.T) {
	t0 := TestGobT{}
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	dec := gob.NewDecoder(buf)

	t1 := TestGobT{1, "1", true}
	enc.Encode(t1)
	dec.Decode(&t0)
	fmt.Printf("%+v\n", t0)

	// If t0 is a new entity, the second decode into t can product expected result: {X:2 Y:2 Z:false}
	// otherwise, t0's bool member has not been changed after the second decode.
	// t0 = TestGobT{}
	t2 := TestGobT{2, "2", false}
	enc.Encode(t2)
	dec.Decode(&t0)
	fmt.Printf("%+v\n", t0)

	// result:
	// {X:1 Y:1 Z:true}
	// {X:2 Y:2 Z:true}

	// If a field has the zero value for its type (except for arrays; see above), it is omitted from the transmission.
	// And "false" is zero value.
	// 所以这个测试提示我们不要解码到同一个对象中，这样有些信息就会被忽略，又没有覆盖该对象的某些字段，
	// 造成解码得到的信息混乱。
	// 所以，不要为了所谓的“节省内存”而去尽量避免创建局部变量，而重复使用同一个变量，有时就会出现隐晦的错误。
}

func sendRPC() bool {
	time.Sleep(5 * time.Second)
	return true
}

// https://stackoverflow.com/questions/56249990/why-write-channel-blocked-in-spite-of-a-goroutine-is-selecting-on-this-channel
func TestSelectChannel(t *testing.T) {
	done := make(chan struct{}, 1)
	ch := make(chan bool)

	go func() { // goroutine A
		select {
		case ch <- sendRPC():
		case <-done:
			fmt.Println("exit")
		}
		// 上面这段代码相当于：
		// ok := sendRPC()
		// select {
		// case ch <- ok:
		// case <-done:
		// 	fmt.Println("exit")
		// }
	}()

	select {
	case <-ch:
	case <-time.After(1000 * time.Millisecond):
		fmt.Println("timeout")
		if len(done) == 0 {
			fmt.Println("1")
			// here write done channel will block until sendRPC() return, why?
			// I expect that the write is nonblock because goroutine A is select on done channel.
			done <- struct{}{}
			fmt.Println("2")
		}
	}

	// result:
	// timeout (after about 1 second)
	// 1
	// exit    (after about 5 second, I expect that it is printed after about 1 second too.)
	// 2
}

func TestVariableDelivery(t *testing.T) {
	var ch1 chan bool
	for i := 0; i < 3; i++ {
		ch1 = make(chan bool) // 新的实体。
		go func() {
			time.Sleep(1 * time.Second)
			fmt.Println(&ch1)
		}() // 传引用变量ch1本身的引用，ch1的值的改变可以被所有引用ch1的线程看/观察到。
		// 于是结果就是三个线程引用同一个引用变量/标签，所以就使用的是同一个标签上的同一个channel实体。
	}
	time.Sleep(2 * time.Second)
	// 0xc00000c028
	// 0xc00000c028
	// 0xc00000c028

	for i := 0; i < 3; i++ {
		ch2 := make(chan bool) // 新的实体，还有新的引用变量。
		go func() {
			time.Sleep(1 * time.Second)
			fmt.Println(&ch2)
		}() // 传引用变量ch2本身的引用，但每次循环创建一个新的引用变量ch2，所以三个线程各自引用三个不同的引用变量，以及三个不同的channel实体。
	}
	time.Sleep(2 * time.Second)
	// 0xc00000c038
	// 0xc00000c048
	// 0xc00000c040

	// 当然也可以传递引用变量ch3本身的拷贝，而不是其引用，这样可以不用每次创建一次引用变量，当然还是上一种方便一点。
	var ch3 chan bool
	for i := 0; i < 3; i++ {
		ch3 = make(chan bool) // 新的实体，还有新的引用变量
		go func(ch3 chan bool) {
			time.Sleep(1 * time.Second)
			fmt.Println(&ch3)
		}(ch3) // 传引用变量ch2本身的引用，但每次循环创建一个新的引用变量ch2，所以三个线程各自引用三个不同的引用变量，以及三个不同的channel实体。
	}
	time.Sleep(2 * time.Second)
	// 0xc0000f6000
	// 0xc0000de020
	// 0xc0000de028
}

type TestGobMapT struct {
	X int
	M map[string]string
}

func TestGobMap(t *testing.T) {
	buf := new(bytes.Buffer)
	dec := gob.NewDecoder(buf)
	enc := gob.NewEncoder(buf)
	t0 := TestGobMapT{X: 1, M: map[string]string{"1": "one", "2": "two", "3": "three"}}
	err := enc.Encode(t0)
	if err != nil {
		panic(err)
	}

	fmt.Println(buf.Bytes())

	// ok
	// t1 := TestGobMapT{}
	// err = dec.Decode(&t1)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("%+v\n", t1)

	// panic: gob: decoding into local type *map[int]string, received remote type T = struct { X int; M map[string]string; }
	var decodedMap map[int]string
	err = dec.Decode(&decodedMap)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", decodedMap)
}

func TestGobDecode(t *testing.T) {
	s := "12345"
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	dec := gob.NewDecoder(buf)

	enc.Encode(s) // encode时会写buf

	var s1, s2 string
	fmt.Println(buf.Len()) // 9
	dec.Decode(&s1)        // decode时会读buf
	fmt.Println(s1)        // "12345"
	fmt.Println(buf.Len()) // 0

	dec.Decode(&s2) // buf中的内容已被读出，“不存在了”
	fmt.Println(s2) // ""
}

func TestPrintInterface(t *testing.T) {
	x := 1
	s := "abc"
	a := []float32{1.321, 1.12e-3}
	fmt.Printf("%v\t%v\t%v\n", x, s, a)
	// 1	abc	[1.321 0.00112]
	// 打印一个interface类型可以用%v。
	// %v	the value in a default format
	// 		when printing structs, the plus flag (%+v) adds field names
}

func do(mu *sync.Mutex) {
	mu.Lock() // 加锁
	fmt.Println("A")
	mu.Unlock()
}

func TestDeadlock1(t *testing.T) {
	mu := new(sync.Mutex)
	ch := make(chan struct{})
	go func() {
		// 线程A
		for {
			select {
			case <-ch:
			}
			time.Sleep(100 * time.Millisecond)
			do(mu)
		}
	}()

	go func() {
		// 线程B1
		mu.Lock()
		defer mu.Unlock()
		// do something ...
		ch <- struct{}{} // 通知另一个线程
		fmt.Println("B1")
	}()

	go func() {
		// 线程B2
		mu.Lock()
		defer mu.Unlock()
		// do something ...
		ch <- struct{}{} // 通知另一个线程
		fmt.Println("B2")
	}()

	time.Sleep(5 * time.Second)

	// 上面这个死锁场景可以总结为：持锁写channel（阻塞操作），目标线程并没有正在接受，而也在试图加锁。
	// 并发编程的一个原则是不要在持有锁时进行任何可能阻塞的操作。
	// 解决方法可以把线程A写ch开一个线程，或者先解锁。
	// 当然，如果代码执行顺序比较重要的话，就不能另开一个线程，只能先解锁了。
}

func TestBoundary(t *testing.T) {
	mu := new(sync.Mutex)
	a := []int{1, 2, 3, 4, 5}
	go func() {
		mu.Lock()
		start := 1
		nToApply := 3
		// if start < 0 || start+nToApply >= len(a) { // 错误
		// 怎么快速知道上面第二个条件是>=还是>呢？
		// 看上下文，下面我们访问a[start:start+nToApply]，即闭开区间[start:start+nToApply)，
		// 也即是start+nToApply = len(a)是可以的，所以是>。
		if start < 0 || start+nToApply > len(a) {
			mu.Unlock()
			return
		}
		toApply := make([]int, nToApply)
		copy(toApply, a[start:start+nToApply])
		mu.Unlock()
	}()
	time.Sleep(1 * time.Second)
}

func TestAppendCopy(t *testing.T) {
	a := []int{1, 2, 3, 4, 5, 6, 7}
	b := make([]int, 1)
	copy(b[1:], a[3:]) // copy不像append，copy不会为b底层重新分配足够大的空间，b[1:]指向的空间为0，所以什么也没拷贝。
	fmt.Printf("a = %v, b = %v\n", a, b)
	c := make([]int, 1)
	c = append(c[1:], a[3:]...) // append会为c底层重新分配足够大的空间，此时c指向了另一个实体。
	fmt.Printf("a = %v, c = %v\n", a, c)
	c[0] = -1
	fmt.Printf("a = %v, c = %v\n", a, c)

	// 正确的做法：
	var d []int // d := []int{}
	d = append(d, a[3:]...)
	fmt.Printf("a = %v, d = %v\n", a, d)
	d = append([]int{1, 2}, d...) // prepend
	fmt.Printf("a = %v, d = %v\n", a, d)

	// a = [1 2 3 4 5 6 7], b = [0]
	// a = [1 2 3 4 5 6 7], c = [4 5 6 7]
	// a = [1 2 3 4 5 6 7], c = [-1 5 6 7]
	// a = [1 2 3 4 5 6 7], d = [4 5 6 7]
	// a = [1 2 3 4 5 6 7], d = [1 2 4 5 6 7]

	// 关于append还要注意：
	x := make([]int, 30) // make([]T, len)
	fmt.Printf("x = %v\n", x)
	x = append(x, 2)
	fmt.Printf("x = %v\n", x)
	// x = [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
	// x = [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 2]
	// 结果并不是：x = [2]

	y := make([]int, 0, 30) // make([]T, len, cap)
	fmt.Printf("y = %v\n", y)
	y = append(y, 2)
	fmt.Printf("y = %v\n", y)
	// y = []
	// y = [2]

	// 数组的默认初始化，即根据每个元素类型对其中每个元素进行默认初始化。
	bools := make([]bool, 5)
	fmt.Printf("bools = %v\n", bools)
	// bools = [false false false false false]

	structs := make([]struct{}, 5)
	fmt.Printf("structs = %v\n", structs)
	// structs = [{} {} {} {} {}]
	fmt.Println(structs[0] == struct{}{})
	// true
}

func TestCommunicateWithChannel(t *testing.T) {
	// 创建一个channel实体，传递引用给其它线程，这样其它线程就可以通过底层这同一个channel实体与本线程交互。
	ch := make(chan string)
	go func(ch chan string) {
		// do something
		ch <- "finish 1"
	}(ch)
	go func(ch chan string) {
		// do something
		ch <- "finish 2"
	}(ch)
	time.Sleep(1 * time.Second)
}

func TestNTasks(t *testing.T) {
	nTasks := 5

	i := 0
	for {
		// do something
		i++
		if i == nTasks {
			break
		}
	}

	// 另一种写法（推荐）
	for {
		// do something
		nTasks--
		if nTasks == 0 {
			break
		}
	}
}

func TestDefer(t *testing.T) {
	// go的defer是在函数作用域结束后才执行，而不是块作用域
	if true {
		defer print("1")
	}
	print("2")

	// 结果：21
}

func TestSelect(t *testing.T) {
	// 错误的代码，两个case并不会如期望地共享同一个代码块。
	// select {
	// case <-rf.leaderEventLoopDone:
	// case <-rf.shutdown:
	// 	return
	// }
}

type testReference struct {
	s string
}

func TestReference(t *testing.T) {
	// 引用变量就像标签，贴在实体对象上。

	data := map[rune]int{'a': 1, 'v': 3, 'd': 19}
	ref := data // 浅复制
	data['x'] = 8
	println(data)
	println(ref)
	// 地址相同，引用变量就像贴在实体对象上的标签。
	// 0xc0000b9f68
	// 0xc0000b9f68

	fmt.Printf("%v\n", data)
	fmt.Printf("%v\n", ref)

	// 多个对同一实体的引用，实体的修改可被这些引用观察(observe)到。
	// map[97:1 100:19 118:3 120:8]
	// map[97:1 100:19 118:3 120:8]

	obj := testReference{"dsa"}
	nonRef := obj // 与前面不同，这些不是引用变量，赋值是深拷贝。
	p1 := &obj    // 如果想浅拷贝，可以使用指针。
	println(&obj)
	println(&nonRef)
	println(p1)
	nonRef.s = "xxx"
	fmt.Printf("%v\n", obj)
	fmt.Printf("%v\n", nonRef)
	fmt.Printf("%v\n", *p1)
}
