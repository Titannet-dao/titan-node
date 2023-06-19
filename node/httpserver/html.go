package httpserver

const webHTML = `
<!DOCTYPE html>
<html>
<head>
	<style>
	table {border-collapse: collapse; width: %s; }
		th, td {
			border: 1px solid #ccc;
			padding: 8px;
		}

		tr:nth-child(even) {
			background-color: #f2f2f2;
		}
	</style>
</head>
<body>
	<h1>list files for %s</h1>
	<table>
		%s
	</table>
</body>
</html>
`
