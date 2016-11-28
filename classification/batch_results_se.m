tic;
repeats = 8;

index = get_index(0, 0:8);
connector = 1:8;
recursive = 9:584;

cmp = intersect(connector, index);
rmp = intersect(recursive, index);

tsplit = 'ss';
output = zeros(1,10);
result = zeros(2,3,10,10,5,2);
% 1D: Mode              Homogeneous=1, Heterogeneou=2
% 2D: Method            CMP=1, RMP=2, CRMP=3
% 3D: Gamma_A           10%=1, ..., 100%=10
% 4D: Gamma_T           10%=1, ..., 100%=10
% 5D: Measure           Acc=1, Pre=2, Rec=3, F1=4, Auc=5
% 6D: Mean/Std          Mean=1, Std=2
for gamma_a = 80
    for gamma_t = 100
        dataset = sprintf('../data/crmp/ss/dataset_%s_%d_%d', tsplit, gamma_a, gamma_t);    
        load(dataset);
        fprintf('------------load %s-----------\n', dataset);
    
        disp('CMP-NP');
        Xtemp = X(:,cmp);
        [means,stds] = get_result(repeats, Xtemp, Y);     
        result(2,1,gamma_a/10,gamma_t/10,:,1) = means;
        result(2,1,gamma_a/10,gamma_t/10,:,2) = stds;
        output([1 3 5 7 9]) = means;
        output([2 4 6 8 10]) = stds;
        fprintf('%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\n',output);

        disp('RMP-NP');
        Xtemp = X(:,rmp);
        [means,stds] = get_result(repeats, Xtemp, Y);
        result(2,2,gamma_a/10,gamma_t/10,:,1) = means;
        result(2,2,gamma_a/10,gamma_t/10,:,2) = stds;
        output([1 3 5 7 9]) = means;
        output([2 4 6 8 10]) = stds;
        fprintf('%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\n',output);
    
        disp('CRMP-NP');
        Xtemp = X(:,index);
        [means,stds] = get_result(repeats, Xtemp, Y);
        result(2,3,gamma_a/10,gamma_t/10,:,1) = means;
        result(2,3,gamma_a/10,gamma_t/10,:,2) = stds;
        output([1 3 5 7 9]) = means;
        output([2 4 6 8 10]) = stds;
        fprintf('%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\n',output);
    end
end
save('results_ss_p', 'result')
toc;