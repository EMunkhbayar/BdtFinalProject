package cs523;

import java.io.Serializable;

import lombok.Data;

@Data
public class DsSalariesDTO  implements Serializable{
	private static final long serialVersionUID = 1L;
	private String id;
	private String workYear;
	private String experienceLevel;
	private String jobTitle;
	private String salary;
	private String salaryCurrency;
	private String salaryInUsd;
	private String companySize;
}
