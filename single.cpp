#include <iostream>
#include <vector>
#include <fstream>
#include "MyTimer.cpp"
using namespace std;
// 331152
// cost time :44527597
int count = 50000;
int package_size=30000;
int main(){
    ifstream V,W;
    V.open("in_V.txt");
    W.open("in_w.txt");
    vector<int> value,weight;
    int a , b;
    for(int i = 0 ; i < count ; i++){
        V >> a;
        W >> b;
        value.push_back(a);
        weight.push_back(b);
    }
    // for(int i:value)cout<<i<<" ";cout<<endl;
    MyTimer time;
    time.Start();
    vector<vector<long long>> dp(2,vector<long long>(package_size + 1,0));
    long long n=0;
    for(int i=1;i<=count;i++){
        for(int j=1;j <= package_size;j++){
            dp[i%2][j] = dp[(i-1)%2][j];
            if(j>=weight[i-1]){
                dp[i%2][j]=max(dp[i%2][j],dp[(i-1)%2][j-weight[i-1]]+value[i-1]);
            }
        }
    }
    time.End();
    V.close();
    W.close();
    cout<<dp[(count)%2][package_size]<<endl;
    cout<<"cost time :"<<time.costTime<<endl;
    cout<<n<<endl;
}
