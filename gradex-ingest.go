//
// ingest exam submissions from different sources and rename all output files consistently
//
// usage:
//
//  gradex-ingest -deadline=2020-04-22-16-00 -classlist=MATH00000_enrolment.csv learndir=MATH00000 outputdir=MATH00000_examno
//
//  * classlist is a csv that should have columns: UUN, Exam Number, Extra Time (giving the number of minutes allowed)
//  * deadline is used to determine which submissions are late (also taking account of allowance for extra time from classlist)
//  * learndir should be the path to the folder containing the unzipped export from Learn
//  * outputdir should be the path where the anonymised scripts will be placed
//
// workflow:
//
//  1. Unzip the Learn download into learndir, and run the above command.
//  2. Any bad submissions will be left in the learndir. Manually inspect these and where possible, replace all the Learn files for a submission with a single file called "uun.pdf" (where uun is the student's UUN, e.g. s1234567).
//  3. Re-run the above command. This will process the "uun.pdf" files.
//

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"flag"
	"io"
	"regexp"

	"github.com/gocarina/gocsv"
	"github.com/georgekinnear/parselearn"
)

// Structure for the class list csv
type Students struct {
	StudentID       string  `csv:"UUN"`
	ExamNumber      string  `csv:"Exam Number"`
	ExtraTime      	int     `csv:"Extra Time"`
}

/*
type SubmissionSummary struct {
	UUN			       string  `csv:"UUN"`
	ExamNumber         string  `csv:"ExamNumber"`
	DateSubmitted      string  `csv:"DateSubmitted"`
	LateSubmission     string  `csv:"LateSubmission"`
	ExtraTime	       int     `csv:"ExtraTime"`
	Filename           string  `csv:"Filename"`
	NumberOfFiles      int     `csv:"NumberOfFiles"`
}
*/

func main() {

// Check arguments

    var courseCode string
    flag.StringVar(&courseCode, "course", "MATH00000", "the course code, will be prepended to output file names")
	
	var classListCSV string
    flag.StringVar(&classListCSV, "classlist", "MATH00000_enrolment.csv", "csv file containing the student UUN, Exam Number and number of minutes of extra time they are entitled to")
	
	var learnDir string
    flag.StringVar(&learnDir, "learndir", "learn_dir", "path of the folder containing the unzipped Learn download")
	
	var outputDir string
    flag.StringVar(&outputDir, "outputdir", "output_dir", "path of the folder where output files should go")
	
	var deadline string
    flag.StringVar(&deadline, "deadline", "2020-04-22-16-00", "date and time of the normal submission deadline")
	
	flag.Parse()

	deadline_time, e := time.Parse("2006-01-02-15-04", deadline)
	check(e)
	
	// Add 59 seconds to the deadline, so that a deadline of 12:00 means submissions up to 12:00:59 are on time but 12:01:00 is late
	deadline_time = deadline_time.Add(time.Second * time.Duration(59))
	
	fmt.Println("course: ", courseCode)
	fmt.Println("deadline: ", deadline_time.Format("2006-01-02 at 15:04:05"))	
	fmt.Println("learn folder: ", learnDir)
	fmt.Println("other folders to read: ", flag.Args())
	
	// Check the output directory exists, and if not then make it
	err := ensureDir(outputDir)
	if err != nil {
		os.MkdirAll(outputDir, os.ModePerm)
	}
	err = ensureDir(outputDir)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	
	// Check that the input folder exists
	err = ensureDir(learnDir)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	
	// Parse the class list
	fmt.Println("class list csv: ", classListCSV)
	classListFile, err := os.OpenFile(classListCSV, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Println("File: ",classListFile, err)
		panic(err)
	}
	defer classListFile.Close()

	classlist_raw := []Students{}
	if err := gocsv.UnmarshalFile(classListFile, &classlist_raw); err != nil {
		panic(err)
	}
	// Make this into a map with UUNs as keys
	classlist := map[string]Students{}
	for _, s := range classlist_raw {
		classlist[s.StudentID] = s
		s.StudentID = strings.ToUpper(s.StudentID)
		if !strings.HasPrefix(s.StudentID, "S") {
			// prepend an "S" to the UUN if not there already in the classlist csv
			s.StudentID = "S"+s.StudentID
		}
	}
	
	fmt.Println("class list contains ", len(classlist), "students")
	
	
	// regex to read the UUN that appears in the Learn files
	finduun, _ := regexp.Compile("_(s[0-9]{7})_attempt_")


	// Build map of UUN to a slice of Learn submissions
	var learn_files = map[string][]parselearn.Submission{}
	var num_learn_files int
	filepath.Walk(learnDir, func(path string, f os.FileInfo, _ error) error {
		if !f.IsDir() {
			r, err := regexp.MatchString(".txt", f.Name())
			if err == nil && r {
				//fmt.Println(f.Name())
				extracted_uun := strings.ToUpper(finduun.FindStringSubmatch(f.Name())[1])
				
				// read the Learn receipt file
				submission, err := parselearn.ParseLearnReceipt(learnDir+"/"+f.Name())
				check(err)
				submission.ExamNumber = classlist[extracted_uun].ExamNumber
				submission.ExtraTime = classlist[extracted_uun].ExtraTime
				submission.ReceiptFilename = f.Name()
				
				// Decide if the submission is LATE or not
				sub_time, _ := time.Parse("2006-01-02-15-04-05", submission.DateSubmitted)
				if(sub_time.After(deadline_time)) {
					if(submission.ExtraTime > 0) {
						// For students with extra time noted in the class list, their submission deadline is shifted
						if(sub_time.After(deadline_time.Add(time.Minute * time.Duration(submission.ExtraTime)))) {
							submission.LateSubmission = "LATE"
						}
					} else {
						// For students with no allowance of extra time, their submission is marked late
						submission.LateSubmission = "LATE"
					}
				}
				
				// If there are already submissions from this student, add them to the list; otherwise start a new list
				if _, ok := learn_files[extracted_uun]; ok {
					learn_files[extracted_uun] = append(learn_files[extracted_uun], submission)					
				} else {
					learn_files[extracted_uun] = []parselearn.Submission{submission}
				}
				num_learn_files++
			}
			}
		return nil
	})
	fmt.Println("learn files: ",num_learn_files, "from", len(learn_files), "students")
		
/*	
	// Read the class list csv	
	csvfile, err := os.Open(classListCSV)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}
	classlistcsv := csv.NewReader(csvfile)
	
	var examno = map[string]string{}
*/

	// Prepare data structures to hold the data
	var submissions []parselearn.Submission
	var bad_submissions []parselearn.Submission
	var no_submissions []parselearn.Submission
	var submission_summaries []parselearn.Submission

	//
	// Identify the submission for each student in the class list
	//
	for _, student := range classlist {
		
		student_uun := student.StudentID
		if !strings.HasPrefix(student_uun, "S") {
			// prepend an "S" to the UUN if not there already in the classlist csv
			student_uun = "S"+student_uun
		}
		student_examno := student.ExamNumber
		extratime := student.ExtraTime
		
		// Check their submissions to Learn
		if student_submissions, ok := learn_files[student_uun]; ok {
			fmt.Printf("%s -> %s (extra time: %d)\n", student_uun, student_examno, extratime)
			
			// Find the last non-LATE submission among student_submissions
			submission := parselearn.Submission{}
			submission.DateSubmitted = "2000-01-01-12-00-00" // a dummy time well in the past
			submission_time, _ := time.Parse("2006-01-02-15-04-05", submission.DateSubmitted)
			submission.LateSubmission = "LATE" // this will appear in the report if there are no on-time submissions
			for _, sub := range student_submissions {
				if sub.LateSubmission == "LATE" {
					// skip any LATE submissions
					fmt.Println(" -- Skipped LATE submission: ", sub.ReceiptFilename)
					sub.ToMark = "No - LATE"
					submission_summaries = append(submission_summaries, sub)
					removeFile(learnDir+"/"+sub.ReceiptFilename)
					if submission.Filename != "" {
						removeFile(learnDir+"/"+sub.Filename)
					}
					continue
				}
				sub_time, _ := time.Parse("2006-01-02-15-04-05", sub.DateSubmitted)
				if sub_time.After(submission_time) {
					// submission is superseded by sub - so remove files for submission
					if submission.ReceiptFilename != "" {
						fmt.Println(" -- Skipped submission: ", submission.ReceiptFilename)
						submission.ToMark = "No - Superseded"						
						submission_summaries = append(submission_summaries, submission)
						removeFile(learnDir+"/"+submission.ReceiptFilename)
						if submission.Filename != "" {
							removeFile(learnDir+"/"+submission.Filename)
						}
					}
					// update submission with the more recent sub
					submission = sub
					submission_time = sub_time
				}				
			}
			
			// If a student's earliest submission is LATE, note that fact
			if submission.LateSubmission == "LATE" {
				fmt.Println(" --- No on-time submission.")
				bad_submissions = append(bad_submissions, student_submissions[0])
				continue
			}
			
			if submission.NumberOfFiles == 1 && submission.FiletypeError == "" {
			
				// We have one PDF for the student, so move it into place in the outputDir
				
				fmt.Println(" -- Using Submission:   ",submission.Filename)
				submission.ToMark = "Yes"
				submission_summaries = append(submission_summaries, submission)
				new_path := outputDir+"/"+student_examno+".pdf"
				if (submission.LateSubmission == "LATE") {
					new_path = outputDir+"/LATE-"+student_examno+".pdf"
				}
				filemovestatus := moveFile(learnDir+"/"+submission.Filename, new_path)
				submission.OutputFile = filemovestatus
				fmt.Println(" --- ", filemovestatus)
				
				// If the file move was OK, we can remove the Learn receipt as it's no longer needed
				if(strings.Contains(filemovestatus, "File")) {
					removeFile(learnDir+"/"+submission.ReceiptFilename)
				}
				
				// Add this record to the table of successes
				submissions = append(submissions, submission)
				
			} else {
				// There was a problem with this submission, so it will need investigation and manual work
				
				fmt.Println(" --- Bad submission: ",submission.NumberOfFiles, " files ", submission.FiletypeError)
				submission.ToMark = "Bad submission"
				submission_summaries = append(submission_summaries, submission)
				bad_submissions = append(bad_submissions, submission)					
			}
			
			// Done - move on to next student
			continue
		}
		
		// At this point they did not submit to Learn - check for a raw UUN.pdf
		raw_uun_path := learnDir+"/"+strings.ToLower(student_uun)+".pdf"
		if _, err := os.Stat(raw_uun_path); err == nil {
			// Such a file exists, so create a dummy Submission for it and then move the PDF into place
			manual_sub := parselearn.Submission{}
			manual_sub.UUN = student_uun
			manual_sub.ExamNumber = student_examno
			filemovestatus := moveFile(raw_uun_path, outputDir+"/"+student_examno+".pdf")
			manual_sub.OutputFile = filemovestatus
			manual_sub.LateSubmission = "Manual"
			submissions = append(submissions, manual_sub)
			
			// Done - move on to next student
			continue
		}
		
		// Now there is really no submission from this student, so record that fact
		sub := parselearn.Submission{}
		sub.UUN = student_uun
		sub.ExamNumber = student_examno
		sub.NumberOfFiles = 0
		no_submissions = append(no_submissions, sub)
	
	}
	
	
	
	
	/*
	
	
	
	
		
		// check the Learn folder
		if learn_file, ok := learn_files[student_uun]; ok {
			fmt.Println(" - Learn file: ",learn_file)

			// read the Learn receipt file
			submission, err := parselearn.ParseLearnReceipt(learnDir+"/"+learn_file[0])
			submission.ExamNumber = student_examno
			submission.ExtraTime = extratime_int
			
			// Decide if the submission is LATE or not
			sub_time, _ := time.Parse("2006-01-02-15-04-05", submission.DateSubmitted)
			if(sub_time.After(deadline_time)) {
				if(extratime_int > 0) {
					// For students with extra time noted in the class list, their submission deadline is shifted
					if(sub_time.After(deadline_time.Add(time.Minute * time.Duration(extratime_int)))) {
						submission.LateSubmission = "LATE"
					}
				} else {
					// For students with no allowance of extra time, their submission is marked late
					submission.LateSubmission = "LATE"
				}
			}
			
			if err == nil {
				if submission.NumberOfFiles == 1 && submission.FiletypeError == "" {
				
					// We have one PDF for the student, so move it into place in the outputDir
					
					fmt.Println(" -- Submission: ",submission.Filename)
					new_path := outputDir+"/"+student_examno+".pdf"
					if (submission.LateSubmission == "LATE") {
						new_path = outputDir+"/LATE-"+student_examno+".pdf"
					}
					filemovestatus := moveFile(learnDir+"/"+submission.Filename, new_path)
					submission.OutputFile = filemovestatus
					fmt.Println(" --- ", filemovestatus)
					
					// If the file move was OK, we can remove the Learn receipt as it's no longer needed
					if(strings.Contains(filemovestatus, "File")) {
						removeFile(learnDir+"/"+learn_file[0])
					}
					
					// Add this record to the table of successes
					submissions = append(submissions, submission)
					
				} else {
					// There was a problem with this submission, so it will need investigation and manual work
					
					fmt.Println(" --- Bad submission: ",submission.NumberOfFiles, " files ", submission.FiletypeError)
					bad_submissions = append(bad_submissions, submission)					
				}
			} else {
				fmt.Printf("Error with %s: %v\n", learn_file, err)
			}
			
		} else {
			// No Learn submission from this student -- check for other sources
			
			// TODO - process for reading in submissions to MS Forms
			
			
			// Last resort: look for manually-created UUN.pdf in the learnDir
			
			raw_uun_path := learnDir+"/"+strings.ToLower(student_uun)+".pdf"
			if _, err := os.Stat(raw_uun_path); err == nil {
				// Such a file exists, so create a dummy Submission for it and then move the PDF into place
				manual_sub := parselearn.Submission{}
				manual_sub.UUN = student_uun
				manual_sub.ExamNumber = student_examno
				filemovestatus := moveFile(raw_uun_path, outputDir+"/"+student_examno+".pdf")
				manual_sub.OutputFile = filemovestatus
				submissions = append(submissions, manual_sub)
			}
		}
		
	}
	
	*/
	
	fmt.Println("\n\nSuccessful submissions: ", len(submissions))
	fmt.Println("\n\nBad submissions: ", len(bad_submissions))
	fmt.Println("\n\nNo submissions: ", len(no_submissions))
	
	// TODO - remove timestamp from filename, and have it as a column in the csv. Make this just append details to csv file if it exists
	report_time := time.Now().Format("2006-01-02-15-04-05")
	parselearn.WriteSubmissionsToCSV(submissions, fmt.Sprintf("%s/%s-learn-success.csv", outputDir, report_time))
	parselearn.WriteSubmissionsToCSV(bad_submissions, fmt.Sprintf("%s/%s-learn-errors.csv", outputDir, report_time))
	parselearn.WriteSubmissionsToCSV(no_submissions, fmt.Sprintf("%s/%s-learn-nosubmission.csv", outputDir, report_time))

	// Write submission summary to csv
	file, err := os.OpenFile(fmt.Sprintf("%s/%s-learn-submissionsummary.csv", outputDir, report_time), os.O_RDWR|os.O_CREATE, os.ModePerm)
	check(err)
	defer file.Close()
	err = gocsv.MarshalFile(&submission_summaries, file)
	check(err)
	
	// That's enough
	os.Exit(0)
	


}

// Move the path_from file to path_to, but only if there is not already a file at path_to
func moveFile(path_from string, path_to string) string {

	// Check path_from exists, and its age
	file_from, err := os.Stat(path_from)
	check(err)
    time_from := file_from.ModTime()
	
	// If there is a file at path_to, check its age. If it is newer than the path_from file, then don't bother copying
	file_to_exists := false
    if file_to, err := os.Stat(path_to); err == nil {
		file_to_exists = true
		time_to := file_to.ModTime()
		if(!time_from.Before(time_to)) {
			// No need to copy over, but delete the path_from file since it is not needed
			removeFile(path_from)
			return "File already exists"
		}
    }
	
	// Now copy the path_from file into the path_to location
	err = CopyFile(path_from, path_to)
	if err != nil {
		fmt.Printf("CopyFile failed %q\n", err)
	} else {
		// Get rid of the path_from file, it's no longer needed
		removeFile(path_from)
		if(file_to_exists) {
			return "File replaced"
		} else {
			return "File created"
		}
	}
	
	return "Done Nothing"
}

func removeFile(path string) {
	err := os.Remove(path)
	check(err)
	return
}

	
func check(e error) {
    if e != nil {
        panic(e)
    }
}



// File copy functions - https://stackoverflow.com/a/21067803

// CopyFile copies a file from src to dst. If src and dst files exist, and are
// the same, then return success. Otherise, attempt to create a hard link
// between the two files. If that fail, copy the file contents from src to dst.
func CopyFile(src, dst string) (err error) {
    sfi, err := os.Stat(src)
    if err != nil {
        return
    }
    if !sfi.Mode().IsRegular() {
        // cannot copy non-regular files (e.g., directories,
        // symlinks, devices, etc.)
        return fmt.Errorf("CopyFile: non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
    }
    dfi, err := os.Stat(dst)
    if err != nil {
        if !os.IsNotExist(err) {
            return
        }
    } else {
        if !(dfi.Mode().IsRegular()) {
            return fmt.Errorf("CopyFile: non-regular destination file %s (%q)", dfi.Name(), dfi.Mode().String())
        }
        if os.SameFile(sfi, dfi) {
            return
        }
    }
    if err = os.Link(src, dst); err == nil {
        return
    }
    err = copyFileContents(src, dst)
    return
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
func copyFileContents(src, dst string) (err error) {
    in, err := os.Open(src)
    if err != nil {
        return
    }
    defer in.Close()
    out, err := os.Create(dst)
    if err != nil {
        return
    }
    defer func() {
        cerr := out.Close()
        if err == nil {
            err = cerr
        }
    }()
    if _, err = io.Copy(out, in); err != nil {
        return
    }
    err = out.Sync()
	
	// Update the "last modified" time on the newly created file
	currenttime := time.Now().Local()
	err = os.Chtimes(dst, currenttime, currenttime)
	if err != nil {
		fmt.Println(err)
	}
    return
}
