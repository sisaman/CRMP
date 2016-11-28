function [Xb,Yb] = balance_data(X,Y)
P = X(Y==1,:);
N = X(Y==-1,:);

pc = length(P);
nc = length(N);

if pc > nc
    ind = randsample(pc, nc);
    P = P(ind,:);
else
    ind = randsample(nc, pc);
    N = N(ind,:);
end

Xb = [N;P];
Yb = ones(length(Xb),1);
Yb(1:length(N)) = -1;
end