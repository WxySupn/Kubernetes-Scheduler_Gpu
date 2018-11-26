package main

import (
	"flag"
	"fmt"
	"k8s.io/api/core/v1"

	//"fmt"
	"os"

	"k8s.io/client-go/kubernetes"

	"path/filepath"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuber "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func GetMaxGpuNode(clientset *kuber.Clientset) (int64,string){
	var(
		i int
		z int
		GpuNum int64
		MaxGpuNum int64
		MaxNode string
	)

	nodes, _ := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	pods, _ := clientset.CoreV1().Pods("").List(metav1.ListOptions{})
	for i = 0; i < len(nodes.Items); i++{
		GpuNum,_ = nodes.Items[i].Status.Allocatable.NvidiaGPU().AsInt64()
		Node_Name := nodes.Items[i].Name
		for z = 0; z < len(pods.Items); z++{
			Pod_GPU_Num, _ := pods.Items[z].Spec.Containers[0].Resources.Limits.NvidiaGPU().AsInt64()
			if (Node_Name == pods.Items[z].Spec.NodeName){
				GpuNum = GpuNum -  Pod_GPU_Num
			}
		}
		if (GpuNum > MaxGpuNum){
			MaxGpuNum = GpuNum
			MaxNode = nodes.Items[i].Name
		}
	}

	return MaxGpuNum,MaxNode
}

func scheduler_agroup(label_work string, clientset *kuber.Clientset){
	pods, _:=clientset.CoreV1().Pods("").List(metav1.ListOptions{})
	var(
		i int
		FirstNodeName string
	)

	GpuNums, NodeName := GetMaxGpuNode(clientset)
	fmt.Println("GPU",GpuNums,"Node",NodeName)
	FirstNodeName = NodeName
	for i = 0; i < len(pods.Items); i++{
		if (GpuNums == 0){
			GpuNums, NodeName = GetMaxGpuNode(clientset)
			fmt.Println("GPU",GpuNums,"Node",NodeName)
		}
		//fmt.Println(label_work, pods.Items[i].ObjectMeta.Labels["name"],pods.Items[i].ObjectMeta.Labels["job"] )
		if (pods.Items[i].ObjectMeta.Labels["name"] == label_work && pods.Items[i].ObjectMeta.Labels["job"] == "worker"){
			fmt.Println("Find a worker pod!")
			pods.Items[i].Spec.NodeName = NodeName
			b := &v1.Binding{
				ObjectMeta: metav1.ObjectMeta{Namespace: pods.Items[i].Namespace, Name: pods.Items[i].Name, UID: pods.Items[i].UID},
				Target: v1.ObjectReference{
					Kind: "Node",
					Name: NodeName,
				},
			}
			clientset.CoreV1().Pods(pods.Items[i].Namespace).Bind(b)
			GpuNums = GpuNums - 1
		}
	}
	for i = 0; i < len(pods.Items); i++{
		if (pods.Items[i].ObjectMeta.Labels["name"] == label_work && pods.Items[i].ObjectMeta.Labels["job"] == "ps"){
			fmt.Println("Find a ps pod!")
			b := &v1.Binding{
				ObjectMeta: metav1.ObjectMeta{Namespace: pods.Items[i].Namespace, Name: pods.Items[i].Name, UID: pods.Items[i].UID},
				Target: v1.ObjectReference{
					Kind: "Node",
					Name: NodeName,
				},
			}
			clientset.CoreV1().Pods(pods.Items[i].Namespace).Bind(b)
			pods.Items[i].Spec.NodeName = FirstNodeName
		}
	}
}

func main(){

	scheduler_name := "gpu_scheduler"
	var kubeconfig *string
	if home := homeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, "kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// uses the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	println("Connect:")
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	println("Find:")
	var i int
	for {
		pods,_:=clientset.CoreV1().Pods("").List(metav1.ListOptions{})
		for i = 0; i < len(pods.Items); i++ {
			//fmt.Println(pods.Items[i].Spec.SchedulerName)
			//fmt.Println(pods.Items[i].Spec.NodeName)
			if (pods.Items[i].Spec.SchedulerName == scheduler_name) && (pods.Items[i].Spec.NodeName == ""){
				fmt.Println("Find a new pod which used our scheduler!")
				time.Sleep(time.Duration(5)*time.Second)
				scheduler_agroup(pods.Items[i].ObjectMeta.Labels["name"], clientset)
				time.Sleep(time.Duration(10)*time.Second)
			}
		}
		time.Sleep(time.Duration(10)*time.Second)
	}

}




