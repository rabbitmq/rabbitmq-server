def dir = new File(project.build.directory)

dir.mkdir()  

// This pattern starts with `.*`. This is normally useless and even
// inefficient but the matching doesn't work without it...
def pattern = ~/.*\.keystore$/
dir.eachFileMatch(pattern) { file ->
  file.delete()
}
