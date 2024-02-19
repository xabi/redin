package main


import c "core:c/libc"
import "core:fmt"
import "core:log"
import "core:math"
import "core:mem"
import "core:net"
import "core:os"
import "core:strconv"
import "core:strings"
import "core:sys/unix"
import "core:thread"
import ds "ds"
import red "red"


buf_size :: 2048

client_worker :: proc(inst: ^red.RedinInst, client_socket: net.TCP_Socket) {
	client_loop: for {
		buf: [buf_size]byte
		builder := strings.builder_from_bytes(buf[:])
		defer strings.builder_destroy(&builder)
		for {
			bytes_read, read_err := net.recv(client_socket, buf[:])
			if read_err != nil {
				break client_loop
			}
			strings.write_bytes(&builder, buf[0:bytes_read])
			if bytes_read == 0 do break client_loop
			if bytes_read < buf_size do break

		}
		commands := strings.to_string(builder)
		command_splits := strings.split(commands, "\r\n")
		// defer delete(command_splits)
		for command in command_splits {
			if len(command) == 0 do break
			if strings.compare(command, "QUIT") == 0 do os.exit(0)
			result := red.red_interpret(inst, command)
			buf := red.result_to_result_string(result)
			defer delete(buf)
			bytes_written, write_err := net.send_tcp(client_socket, transmute([]byte)buf)
			if write_err != nil {
				fmt.println("an error occured while responding to client", write_err)
			}
		}
	}
}

main :: proc() {
	if ODIN_DEBUG {
		// context.logger = log.create_console_logger()
		track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&track, context.allocator)
		context.allocator = mem.tracking_allocator(&track)
		defer {
			if len(track.allocation_map) > 0 {
				fmt.eprintf("=== %v allocations not freed: ===\n", len(track.allocation_map))
				for _, entry in track.allocation_map {
					fmt.eprintf("- %v bytes @ %v\n", entry.size, entry.location)
				}
			}
			if len(track.bad_free_array) > 0 {
				fmt.eprintf("=== %v incorrect frees: ===\n", len(track.bad_free_array))
				for entry in track.bad_free_array {
					fmt.eprintf("- %p @ %v\n", entry.memory, entry.location)
				}
			}
			mem.tracking_allocator_destroy(&track)
		}
	}

	// score_set := ds.score_set_init()
	// ds.insert_or_update(score_set, "bonjour", 100)
	// ds.insert_or_update(score_set, "bonjour", 10)
	// ds.insert_or_update(score_set, "cool", 4)
	// ds.insert_or_update(score_set, "coucou", 12)
	// ds.insert_or_update(score_set, "akey", 100)
	// ds.insert_or_update(score_set, "yooo", 10)
	// ds.insert_or_update(score_set, "aurevoir", 4)
	// ds.insert_or_update(score_set, "ciao", 12)
	// ds.insert_or_update(score_set, "anotherkey", 534)
	// ds.debug_skip_list(score_set.score_to_key^)

	inst := red.make_inst()
	defer free(inst)
	defer delete(inst^)

	endpoint, endpoint_parsed := net.parse_endpoint("0.0.0.0:3000")
	endpoint.port = 3000

	listen_socket, listen_error := net.listen_tcp(endpoint)

	for {
		client_socket, client_endpoint, err := net.accept_tcp(listen_socket)
		if err != nil do break
		t := thread.create_and_start_with_poly_data2(
			inst,
			client_socket,
			client_worker,
			context,
			thread.Thread_Priority.Normal,
			true,
		)
	}

}
