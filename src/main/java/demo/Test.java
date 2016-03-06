package demo;

/**
 * @author Clint
 * @company www.zbj.com
 * @comment
 * @date 2016-02-02.
 */
public class Test implements java.io.Serializable{
    private String systime;
    private int number;


    public Test(String systime, int number) {
        this.systime = systime;
        this.number = number;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public String getSystime() {
        return systime;
    }

    public void setSystime(String systime) {
        this.systime = systime;
    }


}
