public class MutilThread{
    public static void main(String[] args) {
        Runnable r1=new MyRunnable("A");
        Runnable r2=new MyRunnable("B");
        Thread t1=new Thread(r1);
        Thread t2=new Thread(r2);
        t1.start();
        t2.start();
    }
}

class MyRunnable implements Runnable{
    private String word;
    public MyRunnable(String word){
        this.word=word;
    }
    public void run(){
        for(int i=0;i<5;++i)
            System.out.println(word);
    }
    public void test1(int k){
        if(k==1)
            throw new RuntimeException("???");
    }
}