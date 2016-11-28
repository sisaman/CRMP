function result = learn(X, Y)    
    p = cvpartition(Y, 'KFold', 5);    
    m = fitcsvm(X,Y, 'KernelFunction', 'Linear', 'CVPartition', p, ...
            'Standardize', true, 'BoxConstraint', 10);
    result = mean(kfoldfun(m, @evaluate));
end
