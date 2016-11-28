function E = evaluate( model, ~, ~, ~, Xtest, Ytest, ~ )
    [Ypredict,scores] = predict(model, Xtest);
    C = confusionmat(Ytest, Ypredict);
    TP = C(1,1);
    FN = C(1,2);
    FP = C(2,1);
    TN = C(2,2);
    accuracy = (TP + TN) / (TP + TN + FP + FN);
    [~,~,~,auc] = perfcurve(Ytest, scores(:,2), 1);
    precision = TP / (TP + FP);
    recall = TP / (TP + FN);
    f1 = 2*precision*recall / (precision + recall);
    E = [accuracy, precision, recall, f1, auc];
end

