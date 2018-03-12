package player.data.dataseed.bdp;

import java.util.Date;

/**
 * 用户购买行为数据表
 * <p>
 * Created by yaoo on 3/12/18
 */
public class ConsumerBehavior {

    private String userId;
    private Date registerDate;
    private String gender;
    private int age;
    private String province;
    private String city;
    private String telecom;
    private int loginCount;
    private Date lastLoginDate;
    private int consumeCount;
    private int repeatedConsumeCount;
    private int totalPaidAmount;
    private Date lastConsumeDate;
    private boolean hasConsumedonRegister;
    private int amountPaidonRegister;

    @Override
    public String toString() {
        return "ConsumerBehavior{" +
                "userId='" + userId + '\'' +
                ", registerDate=" + registerDate +
                ", gender='" + gender + '\'' +
                ", age=" + age +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", telecom='" + telecom + '\'' +
                ", loginCount=" + loginCount +
                ", lastLoginDate=" + lastLoginDate +
                ", consumeCount=" + consumeCount +
                ", repeatedConsumeCount=" + repeatedConsumeCount +
                ", totalPaidAmount=" + totalPaidAmount +
                ", lastConsumeDate=" + lastConsumeDate +
                ", hasConsumedonRegister=" + hasConsumedonRegister +
                ", amountPaidonRegister=" + amountPaidonRegister +
                '}';
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Date getRegisterDate() {
        return registerDate;
    }

    public void setRegisterDate(Date registerDate) {
        this.registerDate = registerDate;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getTelecom() {
        return telecom;
    }

    public void setTelecom(String telecom) {
        this.telecom = telecom;
    }

    public int getLoginCount() {
        return loginCount;
    }

    public void setLoginCount(int loginCount) {
        this.loginCount = loginCount;
    }

    public Date getLastLoginDate() {
        return lastLoginDate;
    }

    public void setLastLoginDate(Date lastLoginDate) {
        this.lastLoginDate = lastLoginDate;
    }

    public int getConsumeCount() {
        return consumeCount;
    }

    public void setConsumeCount(int consumeCount) {
        this.consumeCount = consumeCount;
    }

    public int getRepeatedConsumeCount() {
        return repeatedConsumeCount;
    }

    public void setRepeatedConsumeCount(int repeatedConsumeCount) {
        this.repeatedConsumeCount = repeatedConsumeCount;
    }

    public int getTotalPaidAmount() {
        return totalPaidAmount;
    }

    public void setTotalPaidAmount(int totalPaidAmount) {
        this.totalPaidAmount = totalPaidAmount;
    }

    public Date getLastConsumeDate() {
        return lastConsumeDate;
    }

    public void setLastConsumeDate(Date lastConsumeDate) {
        this.lastConsumeDate = lastConsumeDate;
    }

    public boolean isHasConsumedonRegister() {
        return hasConsumedonRegister;
    }

    public void setHasConsumedonRegister(boolean hasConsumedonRegister) {
        this.hasConsumedonRegister = hasConsumedonRegister;
    }

    public int getAmountPaidonRegister() {
        return amountPaidonRegister;
    }

    public void setAmountPaidonRegister(int amountPaidonRegister) {
        this.amountPaidonRegister = amountPaidonRegister;
    }
}
