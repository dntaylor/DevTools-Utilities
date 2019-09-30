<!DOCTYPE html>
<html>
<head>
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css">
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.2/jquery.min.js"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js"></script>
<script>
$(document).ready(function(){
    $('.dropdown-submenu a.test').on("click", function(e){
        $(this).next('ul').toggle();
        e.stopPropagation();
        e.preventDefault();
    });
});

$(function() {
    $('.pop').on('click', function() {
        $('.imagepreview').attr('src', $(this).find('img').attr('src'));
        $('#imagemodal').modal('show');   
    });     
});
</script>
<style>
.dropdown-submenu {
    position: relative;
}

.dropdown-submenu .dropdown-menu {
    top: 0;
    left: 100%;
    margin-top: -1px;
}

// prevent scrolling
//body.modal-open {
//    overflow: hidden;
//    position: fixed;
//}
</style>
<?php

$dirFilter = $imgFilter = "";

function url($arguments){
    if(isset($_SERVER['HTTPS'])){
        $protocol = ($_SERVER['HTTPS'] && $_SERVER['HTTPS'] != "off") ? "https" : "http";
    }
    else{
        $protocol = 'http';
    }
    $query = $_GET;
    foreach ($arguments as $key => $value) {
        $query[$key] = $value;
    }
    $newQuery = http_build_query($query);
    $newURL = $protocol . "://" . $_SERVER['SERVER_NAME'] . $_SERVER['PHP_SELF'] . "?" . $newQuery;
    return $newURL;
}


function getFiles($baseDir) {
    return glob("$baseDir/*.png");
}

function checkDir($baseDir) {
    global $dirFilter;
    if (!empty($dirFilter)) {
        return fnmatch($dirFilter,$baseDir);
    }
    else {
        return false;
    }
}

function checkImage($image) {
    global $imgFilter;
    $imageName = basename($image,".png");
    if (!empty($imgFilter)) {
        return fnmatch($imgFilter,$imageName);
    }
    else {
        return true;
    }
}

function displayImages($baseDir) {
    if (!checkDir($baseDir)) {
        return;
    }
    global $dirMap;
    $numImages = count($dirMap[$baseDir]);
    if (!$numImages) {
        return;
    }
    print "<p>";
    print "<div>";
    print "<div style=\"clear:left\"><h3>$baseDir</h3></div>";
    foreach ($dirMap[$baseDir] as $image) {
        if (checkImage($image)) {
            $imageName = basename($image,".png");
            print "<div style=\"float:left\">";
            print "<center>$imageName</center><br/>";
            print "<a href=\"javascript:void(0)\" class=\"pop\"><img src=\"$image\" style=\"border: none; width: 40ex; \"></a><br\>";
            print "<center>[<a href=\"pdf/$baseDir/$imageName.pdf\">pdf</a>] - [<a href=\"pdf/$baseDir/$imageName.root\">root</a>]</center>";
            print "</div>";
        }
    }
    print "</div>";
    print "</p>";
    print "<br/><br/>";
}

$dirMap = array();

$dirFilter = $_GET["dirFilter"];
$imgFilter = $_GET["imgFilter"];

function addLink($dir) {
    global $dirMap;
    $dirName = substr($dir,4);
    $images = getFiles($dir);
    $dirMap[$dirName] = $images;
    $numImages = count($images);
    if ($numImages) {
        $newURL = url(array("dirFilter"=>$dirName));
        print "<li><a tabindex=\"-1\" href=\"$newURL\">$dirName</a></li>";
    }
}

function addSubMenu($dir) {
    $subDirs = glob("$dir/*",GLOB_ONLYDIR);
    if (count($subDirs)) {
        $dirName = substr($dir,4);
        print "<li class=\"dropdown-submenu\">";
        print "<a class=\"test\" tabindex=\"-1\" href=\"#\">$dirName<span class=\"caret\"></span></a>";
        print "<ul class=\"dropdown-menu\">";
        addLink($dir);
        foreach ($subDirs as $subDir) {
            addSubMenu($subDir);
        }
        print "</ul>";
        print "</li>";
    }
    else {
        addLink($dir);
    }
}

function buildMenu() {
    print "<div class=\"dropdown\">";
    print "<ul class=\"nav nav-tabs\">";
    foreach (glob("png/*",GLOB_ONLYDIR) as $dir) {
        addSubMenu($dir);
    }
    print "</ul>";
    print "</div>";
}

function display() {
    global $dirMap;
    foreach ($dirMap as $dir => $images) {
        displayImages($dir);
    }
}

?>
</head>

<body>

<div class="allContent">

<?php buildMenu(); ?>
<br/>

<div class="form">
<form method="get" action="<?php echo htmlspecialchars($_SERVER["PHP_SELF"]);?>">  
  Directory filter: <input type="text" name="dirFilter" value="<?php echo $dirFilter;?>">
  Image filter: <input type="text" name="imgFilter" value="<?php echo $imgFilter;?>">
  <input type="submit" value="Submit">  
</form>
</div>

<div class="content">
<?php display(); ?>
</div>

</div>

<div class="modal fade" id="imagemodal" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
  <div class="modal-dialog">
    <div class="modal-content">              
      <div class="modal-body">
        <button type="button" class="close" data-dismiss="modal"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button>
        <img src="" class="imagepreview" style="width: 100%;" >
      </div>
    </div>
  </div>
</div>

</body>
</html>
