function [means, stds] = get_result(n, X, Y)
    results = zeros(n,5);
    parfor i = 1:n
        output = learn(X,Y);
        results(i,:) = output;        
    end
    
    means = mean(results);
    stds = std(results);
end