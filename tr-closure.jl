@everywhere function mark_graph(graph, mark, k, islice, jslice)
    for i in islice, j in jslice
        gs = min(graph[i,k],graph[k,j])
        if graph[i,j] <= gs
            graph[i,j] = gs
            mark[i,j] = true
        end
    end
end

@everywhere function change_graph(graph, mark, islice, jslice)
    for i in islice, j in jslice
        if mark[i,j]
            graph[i,j] = 0
        end
    end
end

@everywhere function start_kernel_mark(graph, mark, k)
    mark_graph(graph, mark, k, 1:size(graph,1), 1:size(graph,2))
end

@everywhere function start_kernel_change(graph, mark)
    change_graph(graph, mark, 1:size(graph,1), 1:size(graph,2))
end

@everywhere function slice_data()

end

function transitive_parallel(graphin::Array{Int8,2})
    graph = SharedArray{Int8,2}(copy(graphin))
    graphsize = size(graph)
    markedgraph = SharedArray{Bool}(graphsize, init = S -> S[Base.localindexes(S)] = false)
    

    @sync begin
        procs_list = procs(graph)
        for k in 1:graphsize[1]
            @async remotecall_wait(start_kernel_mark, procs_list[(k%length(procs_list))+1], graph, markedgraph, k)
        end
    end 

    @sync begin
        procs_list = procs(graph)
        for k in 1:graphsize[1]
            @async remotecall_wait(start_kernel_change, procs_list[(k%length(procs_list))+1], graph, markedgraph)
        end
    end 

    return graph
end

containment_graph_little = Array{Int8,2}([
      [0 1 1 1 1]
      [0 0 1 0 0]
      [0 0 0 0 0]
      [0 0 0 0 1]
      [0 0 0 0 0]])

containment_graph = Array{Int8,2}([
      [0 0 0 0 0 0 0 0]
      [0 0 0 0 0 0 1 1]
      [1 1 0 1 1 1 1 1]
      [0 0 0 0 0 0 0 0]
      [0 0 0 0 0 1 0 0]
      [0 0 0 0 0 0 0 0]
      [0 0 0 0 0 0 0 0]
      [0 0 0 0 0 0 1 0]])

# Run once to warm up the GC
transitive_parallel(ones(Int8,10,10))

# Test
@time res1 = transitive_parallel(containment_graph_little)
@time res2 = transitive_parallel(containment_graph)
@time res3 = transitive_parallel(ones(Int8,10,10))
@time res4 = transitive_parallel(ones(Int8,100,100))
@time res5 = transitive_parallel(ones(Int8,1000,1000))

# Verify results
println(res1)
println(res2)
# println(res3)
# println(res4)