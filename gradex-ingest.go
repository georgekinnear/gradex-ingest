//
// ingest exam submissions from different sources and rename all output files consistently
//
// usage:
//
//  gradex-ingest -deadline=2020-04-22-16-00 -classlist=MATH00000_enrolment.csv learndir=MATH00000 outputdir=MATH00000_examno
//
//  * classlist is a csv that should have columns: UUN, Exam Number, First Name, Last Name, Minutes of Extra Time Allowed
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
	"encoding/csv"
	"log"
	"io"
	"regexp"
	"strconv"

	"github.com/timdrysdale/parselearn"
)


func main() {

// Check arguments

    var courseCode string
    flag.StringVar(&courseCode, "course", "MATH00000", "the course code, will be prepended to output file names")
	
	var classList string
    flag.StringVar(&classList, "classlist", "MATH00000_enrolment.csv", "csv file containing the student UUN, Exam Number and number of minutes of extra time they are entitled to")
	
	var learnDir string
    flag.StringVar(&learnDir, "learndir", "learn_dir", "path of the folder containing the unzipped Learn download")
	
	var outputDir string
    flag.StringVar(&outputDir, "outputdir", "output_dir", "path of the folder where output files should go")
	
	var deadline string
    flag.StringVar(&deadline, "deadline", "2020-04-22-16-00", "date and time of the normal submission deadline")
	
	flag.Parse()

	deadline_time, e := time.Parse("2006-01-02-15-04", deadline)
	check(e)
	
	fmt.Println("course: ", courseCode)
	fmt.Println("deadline: ", deadline_time.Format("2006-01-02 at 15:04"))	
	fmt.Println("class list csv: ", classList)
	fmt.Println("learn folder: ", learnDir)
	fmt.Println("folders to read: ", flag.Args())
	
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
	
	// Read the contents of the Learn folder
	err = ensureDir(learnDir)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
		
	// regex to read the UUN that appears in the Learn files
	finduun, _ := regexp.Compile("_(s[0-9]{7})_attempt_")
	
	// Build map of UUN to filename, for each .txt receipt file in the learnDir
	var learn_files = map[string]string{}
	filepath.Walk(learnDir, func(path string, f os.FileInfo, _ error) error {
		if !f.IsDir() {
			r, err := regexp.MatchString(".txt", f.Name())
			if err == nil && r {
				//fmt.Println(f.Name())
				extracted_uun := finduun.FindStringSubmatch(f.Name())[1]
				learn_files[strings.ToUpper(extracted_uun)] = f.Name()
			}
		}
		return nil
	})
	fmt.Println("learn files: ",len(learn_files))
	
	
	// Read the class list csv	
	csvfile, err := os.Open(classList)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}
	classlistcsv := csv.NewReader(csvfile)
	
	// Prepare data structures to hold the data
	var examno = map[string]string{}
	var submissions []parselearn.Submission
	var bad_submissions []parselearn.Submission

	// Process each student in the class list csv
	for {
		// Error catching
		record, err := classlistcsv.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		
		// todo - this relies on the columns being in a certain order - redo using gocarina/gocsv
		student_uun := record[0]
		// TODO - prepend an "S" to the UUN if not there already
		student_examno := record[1]
		extratime := record[4]
		extratime_int, _ :=  strconv.Atoi(extratime)
		fmt.Printf("%s -> %s (extra time: %s)\n", student_uun, student_examno, extratime)
		examno[student_uun] = student_examno
		
		// check the Learn folder
		if learn_file, ok := learn_files[student_uun]; ok {
			fmt.Println(" - Learn file: ",learn_file)

			// read the Learn receipt file
			submission, err := parselearn.ParseLearnReceipt(learnDir+"/"+learn_file)
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
						removeFile(learnDir+"/"+learn_file)
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
	
	fmt.Println("\n\nSuccessful submissions: ", len(submissions))
	fmt.Println("\n\nBad submissions: ", len(bad_submissions))
	
	// TODO - remove timestamp from filename, and have it as a column in the csv. Make this just append details to csv file if it exists
	report_time := time.Now().Format("2006-01-02-15-04-05")
	parselearn.WriteSubmissionsToCSV(submissions, fmt.Sprintf("%s/%s-learn-success.csv", outputDir, report_time))
	parselearn.WriteSubmissionsToCSV(bad_submissions, fmt.Sprintf("%s/%s-learn-errors.csv", outputDir, report_time))

	
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
		if(time_to.After(time_from)) {
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
    return
}
