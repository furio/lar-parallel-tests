@everywhere function mark_graph!(graph, mark, k, islice, jslice)
    for i in islice, j in jslice
        gs = min(graph[i,k],graph[k,j])
        if graph[i,j] <= gs
            graph[i,j] = gs
            mark[i,j] = true
        end
    end
end

@everywhere function change_graph!(graph, mark, islice, jslice)
    for i in islice, j in jslice
        if mark[i,j]
            graph[i,j] = 0
        end
    end
end

@everywhere function start_kernel_mark!(graph, mark, k)
    mark_graph!(graph, mark, k, 1:size(graph,1), 1:size(graph,2))
end

@everywhere function start_kernel_change!(graph, mark, interval_split)
    idx = indexpids(graph)
    change_graph!(graph, mark, interval_split[idx]+1:interval_split[idx+1], 1:size(graph,2))
end

function transitive_reduction_parallel!(graph::SharedArray{Int8,2}, markedgraph::SharedArray{Bool})
    graphsize = size(graph)

    # processes    
    procs_list = procs(graph)
    # graph split
    interval_split = [round(Int, s) for s in linspace(0,size(graph,1),length(procs_list)+1)]    

    @sync begin
        for k in 1:graphsize[1]
            @async remotecall_wait(start_kernel_mark!, procs_list[(k%length(procs_list))+1], graph, markedgraph, k)
        end
    end 

    @sync begin
        for p in procs_list
            @async remotecall_wait(start_kernel_change!, p, graph, markedgraph, interval_split)
        end
    end
end

function transitive_reduction_parallel(graphin::Array{Int8,2})
    graph = SharedArray{Int8,2}(copy(graphin))
    markedgraph = SharedArray{Bool}(size(graph), init = S -> S[Base.localindexes(S)] = false)

    # Time without alloc here
    transitive_reduction_parallel!(graph, markedgraph)

    return graph
end

function transitive_reduction_serial(graphin::Array{Int8,2})
    graph = Array{Int8,2}(copy(graphin))
    n = size(graphin,1)
    for j in 1:n
        for i in 1:n
            if graph[i, j] > 0
                for k in 1:n
                    if graph[j, k] > 0
                        graph[i, k] = 0
                    end
                end
            end
        end
    end
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
transitive_reduction_serial(ones(Int8,10,10))
transitive_reduction_parallel(ones(Int8,10,10))

# Test
@time res1 = transitive_reduction_parallel(containment_graph_little)
@time res2 = transitive_reduction_parallel(containment_graph)
@time res3 = transitive_reduction_parallel(ones(Int8,10,10))
@time res4 = transitive_reduction_parallel(ones(Int8,100,100))
@time res5 = transitive_reduction_parallel(ones(Int8,1000,1000))

@time sres1 = transitive_reduction_serial(containment_graph_little)
@time sres2 = transitive_reduction_serial(containment_graph)
@time sres3 = transitive_reduction_serial(ones(Int8,10,10))
@time sres4 = transitive_reduction_serial(ones(Int8,100,100))
@time sres5 = transitive_reduction_serial(ones(Int8,1000,1000))

# Verify results
println(res1)
println(res2)
# println(res3)
# println(res4)