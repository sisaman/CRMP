tic;
repeats = 10;

index_hm = get_index(0:5, 0:5);
index_ht = get_index(0:7, 0:8);

connector = 1:8;
recursive = 9:584;

cmp_hm = intersect(connector, index_hm);
cmp_ht = intersect(connector, index_ht);

rmp_hm = intersect(recursive, index_hm);
rmp_ht = intersect(recursive, index_ht);

tsplit = 'ss';
output = zeros(1,10);
result = zeros(2,3,10,10,5,2);
% 1D: Mode              Homogeneous=1, Heterogeneous=2
% 2D: Method            CMP=1, RMP=2, CRMP=3
% 3D: Gamma_A           10%=1, ..., 100%=10
% 4D: Gamma_T           10%=1, ..., 100%=10
% 5D: Measure           Acc=1, Pre=2, Rec=3, F1=4, Auc=5
% 6D: Mean/Std          Mean=1, Std=2
for gamma_a = 10:10:80
    for gamma_t = 100
        dataset = sprintf('dataset_%s_%d_%d', tsplit, gamma_a, gamma_t);    
        load(dataset);
        fprintf('------------load %s-----------\n', dataset);
    %     [X,Y] = balance_data(X,Y);

        disp('CMP-Homogeneous');
        Xtemp = X(:,cmp_hm);
        [means,stds] = get_result(repeats, Xtemp, Y);     
        result(1,1,gamma_a/10,gamma_t/10,:,1) = means;
        result(1,1,gamma_a/10,gamma_t/10,:,2) = stds;
        output([1 3 5 7 9]) = means;
        output([2 4 6 8 10]) = stds;
        fprintf('%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\n',output);
        
        disp('CMP-Heterogeneous');
        Xtemp = X(:,cmp_ht);
        [means,stds] = get_result(repeats, Xtemp, Y);     
        result(2,1,gamma_a/10,gamma_t/10,:,1) = means;
        result(2,1,gamma_a/10,gamma_t/10,:,2) = stds;
        output([1 3 5 7 9]) = means;
        output([2 4 6 8 10]) = stds;
        fprintf('%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\n',output);

        disp('RMP-Homogeneous');
        Xtemp = X(:,rmp_hm);
        [means,stds] = get_result(repeats, Xtemp, Y);
        result(1,2,gamma_a/10,gamma_t/10,:,1) = means;
        result(1,2,gamma_a/10,gamma_t/10,:,2) = stds;
        output([1 3 5 7 9]) = means;
        output([2 4 6 8 10]) = stds;
        fprintf('%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\n',output);

        disp('RMP-Heterogeneous');
        Xtemp = X(:,rmp_ht);
        [means,stds] = get_result(repeats, Xtemp, Y);
        result(2,2,gamma_a/10,gamma_t/10,:,1) = means;
        result(2,2,gamma_a/10,gamma_t/10,:,2) = stds;
        output([1 3 5 7 9]) = means;
        output([2 4 6 8 10]) = stds;
        fprintf('%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\n',output);
        
        disp('CRMP-Homogeneous');
        Xtemp = X(:,index_hm);
        [means,stds] = get_result(repeats, Xtemp, Y);
        result(1,3,gamma_a/10,gamma_t/10,:,1) = means;
        result(1,3,gamma_a/10,gamma_t/10,:,2) = stds;
        output([1 3 5 7 9]) = means;
        output([2 4 6 8 10]) = stds;
        fprintf('%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\n',output);        

        disp('CRMP-Heterogeneous');
        Xtemp = X(:,index_ht);
        [means,stds] = get_result(repeats, Xtemp, Y);
        result(2,3,gamma_a/10,gamma_t/10,:,1) = means;
        result(2,3,gamma_a/10,gamma_t/10,:,2) = stds;
        output([1 3 5 7 9]) = means;
        output([2 4 6 8 10]) = stds;
        fprintf('%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\n',output);
    end
end
save('results_ss_a', 'result')
toc;