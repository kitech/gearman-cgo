/*
 * Want it to be a gearman c wrap for go language
 */
package gearmanc

import (
	"fmt"
	"unsafe"
	"bytes"
)

// #include <stdio.h>
// #include <errno.h>
// #include <string.h>
// #include <stdlib.h>
// #include <libgearman/gearman.h>
// // #cgo pkg-config: gearmandaaaa
// #cgo CFLAGS: -I/serv/stow/gearman/include/
// #cgo LDFLAGS: -lgearman -L/serv/stow/gearman/lib/ -Bdynamic -Wl,-dynamic-linker,/serv/stow/gearman/lib -Wl,-rpath,/serv/stow/gearman/lib/
import "C"

func Unused() {
}

func Atest() {
	var i int
	i = 1
	fmt.Println("go int: %v, %v", i, C.GEARMAN_ERROR)

	var gmret C.gearman_return_t = C.GEARMAN_SUCCESS
	var gmch C.gearman_client_st
	var gmchp *C.gearman_client_st = nil
	var gmtask *C.gearman_task_st = nil
	var gmarg C.gearman_argument_t
	var gmres *C.gearman_result_st = nil
	var gmbret C._Bool
	var cvoid unsafe.Pointer

	fmt.Println("cvoid*:", cvoid);

	gmchp, err := C.gearman_client_create(&gmch)

	fmt.Println("abcd:", err, "------", gmchp, (gmchp == &gmch), gmret)

	gmret, err = C.gearman_client_add_server(gmchp, C.CString("10.207.16.251"), 1235);

	gmtask, err = C.gearman_execute(gmchp, C.CString("gmworker_node_10.207.15.65_dummy"),
		C.strlen(C.CString("gmworker_node_10.207.15.65_dummy")), nil, 0, nil, &gmarg, nil)

	gmbret, err = C.gearman_success(uint32(C.gearman_task_return(gmtask)))

	fmt.Println("succ: ", gmbret, err);

	gmres, err = C.gearman_task_result(gmtask)

	fmt.Println("res:", C.GoStringN(C.gearman_result_value(gmres),
		C.int(C.gearman_result_size(gmres))))

	C.gearman_client_free(gmchp)
}

/*
Usage:

*/

const (
	GEARMAN_SUCCESS = C.GEARMAN_SUCCESS
	)

// 非线程安全的，不可以在多个线程中使用同一个实例
type GearmanClient struct {
	client C.gearman_client_st
	clientp *C.gearman_client_st
	debug int 
}

func New() (gc GearmanClient) {
	// var gc GearmanClient;
	gcp := new(GearmanClient)
	gcp.debug = 1
	C.gearman_client_create(&gcp.client)

	gcp.clientp = nil;
	gcp.clientp = &gcp.client;

	gc = *gcp
	return
}

func (gc *GearmanClient) AddServer(host string, port int) int {
	var ret C.gearman_return_t;

	ret = C.gearman_client_add_server(gc.clientp, C.CString(host), C.in_port_t(port))

	fmt.Println("ret:", ret, int(ret) == GEARMAN_SUCCESS);
	return int(ret);
	// return GEARMAN_SUCCESS;
}

func (gc *GearmanClient) Do(function_name string, workload string) (string, int) {
	var ret C.gearman_return_t;
	var rsize C.size_t;
	var gmval unsafe.Pointer = nil
	
	gmval = C.gearman_client_do(gc.clientp, C.CString(function_name), nil, 
		unsafe.Pointer(&workload), C.size_t(len(workload)), &rsize, &ret);

	var rbval []byte = C.GoBytes(gmval, C.int(rsize));
	var rval string = bytes.NewBuffer(rbval).String()

	fmt.Println("rval:", int(rsize), rval);

	return rval, int(ret);
}

func (gc *GearmanClient) DoBackground(function_name string, workload string) (string, int) {
	var ret C.gearman_return_t;
	// var rsize C.size_t;
	// var gmval unsafe.Pointer = nil
	var job_handle [C.GEARMAN_JOB_HANDLE_SIZE]C.char;

	ret = C.gearman_client_do_background(gc.clientp, C.CString(function_name), nil,
		unsafe.Pointer(&workload), C.size_t(len(workload)), &job_handle[0]);

	// var rval string = bytes.NewBuffer(gmval
	var rval string = C.GoString(&job_handle[0]);

	return rval, int(ret);
	// return "", int(ret);
}

func (gc *GearmanClient) Close() {
	C.gearman_client_free(gc.clientp);
}

