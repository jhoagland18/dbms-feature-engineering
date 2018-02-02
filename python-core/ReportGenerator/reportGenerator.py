#!/usr/bin/env python
import os

dir = os.path.dirname(os.path.abspath(__file__))

report= """<!DODCTYPE html>
<html>
	<head>
		<title>Report</title>

		<style>
	
			
			#grid {
				width:100%;
				border-spacing:10px;
			}

			.grid_element {
				width:40%;
				border: 2px solid black;
				padding:2%;
				text-align:center;
				vertical-align: top;

			}
			.section_header {
				text-align:center;
			}

			#summary_text {
				padding:2%;
			}

			img {
				height:250px;
			}
			
		</style>
	</head>

	<body>
		<div id="main_content">
			<div id="report_summary">
				<h2 class="section_header">Report Summary</h2>
				
				<table id="grid">
  					<tr>"""

for x in range (1, 3):
	report+="""<td class="grid_element">
							<img src="""+os.path.join("report-source","IMG"""+str(x)+""".png""")+"""></img>
							<p>""" + open(os.path.join(dir,  "..","..","output","Report","report-source","DESCR"+str(x)+".txt"), 'r').read() + """</p>
						</td>"""

report+="""</tr>
  					<tr>"""

for x in range (3, 5):
	report+="""<td class="grid_element">
							<img src="""+os.path.join("report-source","IMG"""+str(x)+""".png""")+"""></img>
							<p>""" + open(os.path.join(dir, "..","..","output","Report","report-source","DESCR"+str(x)+".txt"), 'r').read() + """</p>
						</td>"""

report+="""</tr>
				</table>
				
			</div>
			<div id="summary_text">
				<p>""" + open(os.path.join(dir, "..","..","output","Report","report-source","text_at_bottom.txt"), 'r').read() + """</p>
			</div>
		</div>
	</body>
	</html>"""

Html_file=open(os.path.join(dir, "..","..","output","Report","Report.html"),"w")
Html_file.write(report)
Html_file.close()
