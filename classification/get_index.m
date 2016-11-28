function index = get_index(rng_src, rng_tgt)
    index = [];
    f = 1;
    for i = 0:8
        if ismember(i,rng_src)
            index = [index, f];
        end
        f = f + 1;
    end

    for i = 0:8
        for j = 0:8
            for k = 0:8
                if ismember(i,rng_src) && ismember(j,rng_src) && ismember(k,rng_tgt)
                    index = [index, f];                    
                end
                f = f + 1;
            end
        end
    end
end
