

**pull preliminary cumulative 2022-23 GPA
global year "2023" //preceding year
global schoolyear "2023-24"
/*----------------------------------------------------------------------------*/

*PULL COURSES & GRADES

*Pull High School Courses and Grades

* Pull StudentMarks
clear
odbc load, exec("SELECT distinct SchoolYear, StudentID, SchoolDBN, TermCD, CourseCD, Section, Credits, Mark, MarkingPeriod FROM SIF.dbo.Hsst_tbl_StudentMarks WHERE SchoolYear = $year and Credits > 0 and IsFinal = 1 and IsExam = 0") dsn("SIF")
sort SchoolYear SchoolDBN TermCD CourseCD MarkingPeriod
tempfile StudentMarks
save `StudentMarks'
* Pull CourseInfo
clear
odbc load, exec("SELECT distinct SchoolYear, SchoolDBN, TermCD, CourseCD, CourseTitle, GradeAverageFactor FROM SIF.dbo.Hsst_tbl_CourseInfo WHERE SchoolYear = $year") dsn("SIF")
sort SchoolYear SchoolDBN TermCD CourseCD
tempfile CourseInfo
save `CourseInfo'
* Pull CourseFlag
clear
odbc load, exec("SELECT distinct SchoolYear, SchoolDBN, TermCD, CourseCD, GradeAveragedFlag, MarkingPeriod FROM SIF.dbo.Hsst_tbl_CourseFlag WHERE SchoolYear = $year") dsn("SIF")
by SchoolYear SchoolDBN TermCD CourseCD MarkingPeriod, sort: gen dup = cond(_N==1,0,_n)
drop if dup>1
drop dup
sort SchoolYear SchoolDBN TermCD CourseCD MarkingPeriod
tempfile CourseFlag
save `CourseFlag'
* Pull School
clear
odbc load, exec("SELECT distinct SchoolDBN, NumericSchoolDBN FROM SIF.STARS.School") dsn("SIF")
sort SchoolDBN
tempfile School
save `School'
* Pull MarkDefinition
clear
odbc load, exec("SELECT distinct SchoolYear, NumericSchoolDBN, Term, Mark, isPassing, AlphaEquivalent, NumericEquivalent FROM SIF.STARS.vw_MarkDefinition WHERE SchoolYear = $year") dsn("SIF")
rename Term TermCD
sort SchoolYear NumericSchoolDBN TermCD Mark
tempfile MarkDefinition
save `MarkDefinition'

* Merge all tables
use `StudentMarks', clear            
* Merge StudentMarks and CourseInfo
merge m:1 SchoolYear SchoolDBN TermCD CourseCD using `CourseInfo'
keep if _merge==3
drop _merge
sort SchoolYear SchoolDBN TermCD CourseCD
* Merge with CourseFlag
merge m:1 SchoolYear SchoolDBN TermCD CourseCD MarkingPeriod using `CourseFlag'
drop if _merge==2
drop _merge
sort SchoolDBN
* Merge with School
merge m:1 SchoolDBN using `School'
keep if _merge==3
drop _merge
sort SchoolYear NumericSchoolDBN TermCD Mark
* Merge with MarkDefinition
merge m:1 SchoolYear NumericSchoolDBN TermCD Mark using `MarkDefinition'
drop if _merge==2
drop _merge NumericSchoolDBN MarkingPeriod
gen dataset="High School"
tempfile hs_coursegrades
save `hs_coursegrades'

*High School and Middle School
gen subject = ""
replace subject = "English" if substr(CourseCD,1,1)=="E"
replace subject = "Social Studies" if substr(CourseCD,1,1)=="H"
replace subject = "Mathematics" if substr(CourseCD,1,1)=="M"
replace subject = "Science" if substr(CourseCD,1,1)=="S"
replace subject = "Foreign Language" if substr(CourseCD,1,1)=="F"
replace subject = "Physical Education & Health" if substr(CourseCD,1,1)=="P"
replace subject = "Arts" if substr(CourseCD,1,1)=="A" | substr(CourseCD,1,1)=="U" | substr(CourseCD,1,1)=="D" | substr(CourseCD,1,1)=="C"
replace subject = "Technology" if substr(CourseCD,1,1)=="T"
replace subject = "Career Development" if substr(CourseCD,1,1)=="R"
replace subject = "Business" if substr(CourseCD,1,1)=="B"
replace subject = "Human Services" if substr(CourseCD,1,1)=="K"
replace subject = "Guidance" if substr(CourseCD,1,1)=="G"
replace subject = "Undefined" if substr(CourseCD,1,1)=="Z"
*Adjust for elementary
replace subject = "" if substr(CourseCD,4,1)=="J"

* Create course_description variable
gen course_description = ""
replace course_description = "Honors" if substr(CourseCD,6,1)=="H"
replace course_description = "Advanced Placement (AP)" if substr(CourseCD,6,1)=="X"
replace course_description = "International Baccalaureate (IB)" if substr(CourseCD,6,1)=="B"
replace course_description = "College-Level: College Credit" if substr(CourseCD,6,1)=="U"
replace course_description = "College-Level: Non-College Credit" if substr(CourseCD,6,1)=="C"
replace course_description = "CTE" if substr(CourseCD,6,1)=="T"
replace course_description = "Non-Credit/Remediation" if substr(CourseCD,6,1)=="S"
replace course_description = "Exam Preparation" if substr(CourseCD,6,1)=="P"
replace course_description = "N/A" if substr(CourseCD,6,1)=="Q"
*Adjust for elementary
replace course_description = "" if substr(CourseCD,4,1)=="J"
replace course_description = "Accelerated" if substr(CourseCD,6,1)=="A" & substr(CourseCD,4,1)=="M"
replace course_description = "" if inlist(substr(CourseCD,6,1),"X","B","U","C","P") & substr(CourseCD,4,1)=="M"

* Rename variables
rename SchoolYear school_year
rename StudentID student_id
rename SchoolDBN dbn
rename TermCD term_code
rename CourseCD course_code
rename CourseTitle course_title
rename GradeAverageFactor grade_average_factor
rename GradeAveragedFlag grade_averaged_flag
rename isPassing is_passing
rename AlphaEquivalent alpha_equivalent
rename NumericEquivalent numeric_equivalent
rename *, lower

order school_year student_id dbn term_code section course_code course_title subject course_description credits mark grade_average_factor grade_averaged_flag is_passing alpha_equivalent numeric_equivalent

tempfile coursegrades
save `coursegrades'

import delimited "R:\Assessment & Accountability\RPSG\SOUPS\Student_Biographic June Status (Split Pea Soup)\DATA\INTERNAL\UNSCRAMBLED\2023-24_June-Biog_PK-12_internal_PRELIM.csv", clear
keep if inlist(grade_level,"09","10","11","12")
drop grade_level
merge 1:m student_id using `coursegrades'
keep if _merge==3
drop _merge
order school_year student_id dbn term_code section course_code course_title subject course_description credits mark grade_average_factor grade_averaged_flag is_passing alpha_equivalent numeric_equivalent
sort student_id
replace is_passing=. if missing(mark)

*calculate GPA
drop if grade_averaged_flag==0 | numeric_equivalent==.
* Generate credits attempted, credits earned and gpa_points
* Overall
gen tot_cred_earned = credits*is_passing
gen tot_gpa_pts = numeric_equivalent*credits*grade_average_factor

rename credits tot_cred_att
collapse(sum) tot_*, by(school_year student_id)

tempfile 2024
save `2024'


*load 2022-23 and 2021-22
local courseyears 2021-22 2022-23
local saveyears 2022 2023
local n : word count `courseyears'

forvalues i = 1/`n' {
	local courseyear : word `i' of `courseyears'
	local saveyear : word `i' of `saveyears'
	
import delimited "R:\Assessment & Accountability\RPSG\SOUPS\Student_Courses & Grades (Avgolemono)\Avgolemono Data\High School\Unscrambled\\`courseyear'_HsCourseAndGrades.csv", clear
*calculate GPA
drop if grade_averaged_flag==0 | numeric_equivalent==.
* Generate credits attempted, credits earned and gpa_points
* Overall
gen tot_cred_earned = credits*is_passing
gen tot_gpa_pts = numeric_equivalent*credits*grade_average_factor

rename credits tot_cred_att
collapse(sum) tot_*, by(school_year student_id)
tempfile `saveyear'
save ``saveyear''
}

use `2022'
append using `2023'
append using `2024'

collapse(sum) tot_*, by(student_id)

gen tot_gpa = tot_gpa_pts/tot_cred_att
replace tot_gpa = round(tot_gpa,0.01)

save "R:\Assessment & Accountability\RPSG\CUNY & COLLEGE READINESS\Ad Hocs\2024_07_24_CUNY Welcome Letter Data Pull\2021-2 to 2023-24 Cumulative GPA.dta", replace
