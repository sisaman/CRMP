tic;
repeats = 10;

index_social = get_index(0:5, 0:5);
index_social_spatial = get_index(0:6, 0:6);
index_social_temporal = get_index([0:5,7], [0:5,7]);
index_social_textual = get_index([0:5,7], [0:5,8]);
index_social_spatial_temporal = get_index(0:7, 0:7);
index_social_spatial_textual = get_index(0:6, [0:6,8]);
index_social_temporal_textual = get_index([0:5,7], [0:5,7:8]);
index_total = get_index(0:7, 0:8);

connector = 1:8;
recursive = 9:584;

index_set = {index_social; index_social_spatial; index_social_temporal;
            index_social_textual; index_social_spatial_temporal;
            index_social_spatial_textual; index_social_temporal_textual;
            index_total};
        
label = {'Social'; 'Social+Spatial'; 'Social+Temporal'; 'Social+Textual';
         'Social+Spatial+Temporal'; 'Social+Spatial+Textual'; 'Social+Temporal+Textual'; 
         'Social+Spatial+Temporal+Textual'};

tsplit = 'ss';
output = zeros(1,10);
result = zeros(8,3,10,10,5,2);
% 1D: Heterogeneity     Homogeneous=1, Heterogeneou=2
% 2D: Method            CMP=1, RMP=2, CRMP=3
% 3D: Gamma_A           10%=1, ..., 100%=10
% 4D: Gamma_T           10%=1, ..., 100%=10
% 5D: Measure           Acc=1, Pre=2, Rec=3, F1=4, Auc=5
% 6D: Mean/Std          Mean=1, Std=2
for mode = 1:8
    index = index_set{mode};
    cmp = intersect(connector, index);
    rmp = intersect(recursive, index);
    for gamma_a = 80
        for gamma_t = 100
            dataset = sprintf('../data/crmp/ss/dataset_%s_%d_%d', tsplit, gamma_a, gamma_t);    
            load(dataset);
            disp(label{mode});
        
            disp('CMP');
            Xtemp = X(:,cmp);
            [means,stds] = get_result(repeats, Xtemp, Y);     
            result(mode,1,gamma_a/10,gamma_t/10,:,1) = means;
            result(mode,1,gamma_a/10,gamma_t/10,:,2) = stds;
            output([1 3 5 7 9]) = means;
            output([2 4 6 8 10]) = stds;
            fprintf('%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\n',output);            

            disp('RMP');
            Xtemp = X(:,rmp);
            [means,stds] = get_result(repeats, Xtemp, Y);
            result(mode,2,gamma_a/10,gamma_t/10,:,1) = means;
            result(mode,2,gamma_a/10,gamma_t/10,:,2) = stds;
            output([1 3 5 7 9]) = means;
            output([2 4 6 8 10]) = stds;
            fprintf('%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\n',output);

            disp('CRMP');
            Xtemp = X(:,index);
            [means,stds] = get_result(repeats, Xtemp, Y);
            result(mode,3,gamma_a/10,gamma_t/10,:,1) = means;
            result(mode,3,gamma_a/10,gamma_t/10,:,2) = stds;
            output([1 3 5 7 9]) = means;
            output([2 4 6 8 10]) = stds;
            fprintf('%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\t%.3f±%0.3f\n',output);        
        end
    end
end
save('results_ss_h', 'result')
toc;