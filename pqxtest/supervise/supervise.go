package supervise

import (
	"log"
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

func Main() {
	pid, _ := strconv.Atoi(os.Getenv("_PQX_SUP_PID"))
	if pid == 0 {
		return
	}

	log.SetFlags(0)
	awaitParentDeath()
	p, err := os.FindProcess(pid)
	if err != nil {
		log.Fatalf("find process: %v", err)
	}
	if err := p.Signal(syscall.Signal(syscall.SIGQUIT)); err != nil {
		log.Fatalf("error signaling process: %v", err)
	}

	os.Exit(0)
}

func awaitParentDeath() {
	_, _ = os.Stdin.Read(make([]byte, 1))
}

func This(pid int) {
	exe, err := os.Executable()
	if err != nil {
		panic(err)
	}
	sup := exec.Command(exe)
	sup.Env = append(os.Environ(), "_PQX_SUP_PID="+strconv.Itoa(pid))
	sup.Stdout = os.Stdout
	sup.Stderr = os.Stderr

	// set a pipe we never write to as to block the supervisor until we die
	_, err = sup.StdinPipe()
	if err != nil {
		panic(err)
	}
	err = sup.Start()
	if err != nil {
		panic(err)
	}

	go func() {
		// Exiting this function without this reference to sup means
		// cmd is sent to the heap, where it will be eligible for GC,
		// possibly too early, causing the stdin pipe to be closed
		// early which will cause the supervisor to exit early, and
		// kill postgres too early, leading test failures, and sad
		// developers.
		_ = sup.Wait()
	}()
}
