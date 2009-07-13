/*
	Copyright (c) 2005, 2006 Rafael Robayna

	Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

	Additional Contributions by: Morris Johns
*/
var CPDrawing = Base.extend({
	canvasPainter: null,  // a reference to the instance of the canvasPainter it is going to manipulate
	drawing: null, // the drawing data
	undoNodes: null, // undone drawing data nodes
	lastClear: null,
	
	constructor: function(canvasPainter) {
		this.canvasPainter = canvasPainter;
		this.drawing = new Array();
		this.undoNodes = new Array();
		this.canvasPainter.addWidgetListener(this.recordAction.bindAsEventListener(this)); 
	},

	recordAction: function() {
		if(this.drawing.length != 0 && this.canvasPainter.cpMouseDownState == true && (this.canvasPainter.curDrawAction == 0 || this.canvasPainter.curDrawAction == 1)) {
			var currentNode = this.drawing[this.drawing.length - 1];
			currentNode.p[currentNode.p.length] = this.canvasPainter.curPos;
		} else {
			if(this.canvasPainter.curDrawAction == 5) this.lastClear = this.drawing.length;
			this.drawing.push(this.addNode());
			this.undoNodes = new Array();
		}
	},

	addNode: function() {
		var drawingNode = new Object()
		drawingNode.p = new Array(2);  //points array
		drawingNode.p[0] = this.canvasPainter.startPos;
		drawingNode.p[1] = this.canvasPainter.curPos;
		drawingNode.a = this.canvasPainter.curDrawAction; //action
		//color and line width should stored and recalled independantly for both this and animator
		drawingNode.c = this.canvasPainter.drawColor; //color
		drawingNode.w = this.canvasPainter.context.lineWidth; //width
		return drawingNode;
	},
	
	removeLastNode: function() {
		if(this.drawing.length == 0) return;
		this.undoNodes.push(this.drawing.pop());
		this.paintDrawing();
	},
	
	addLastRemovedNode: function() {
		if(this.undoNodes.length == 0) return;
		this.drawing.push(this.undoNodes.pop());
		this.paintDrawing();
	},

	paintDrawing: function() {	
		this.canvasPainter.clearCanvas(canvasPainter);
		for(var i = 0; i < this.drawing.length; i++) {
			var drawingNode = this.drawing[i];
			this.canvasPainter.context.fillStyle = drawingNode.c;
			this.canvasPainter.context.strokeStyle = drawingNode.c;
			this.canvasPainter.context.lineWidth = drawingNode.w;

			if(drawingNode.p.length == 2) {
				this.canvasPainter.drawActions[drawingNode.a](drawingNode.p[0], drawingNode.p[1], this.canvasPainter.context, false);
			} else {
				for(var n = 0; n < (drawingNode.p.length - 1); n++) {
					this.canvasPainter.drawActions[drawingNode.a](drawingNode.p[n], drawingNode.p[n+1], this.canvasPainter.context, false);
				}
			}
		}
	},

	saveDrawing: function() {
		if(this.lastClear != null) {
			this.drawing = this.drawing.slice(this.lastClear);
			this.lastClear = null;
		}
		this.optimizeForSave();
		return this.drawing.toSource();
	},

	optimizeForSave: function() {
		//need to implement this
		for(var i = (this.drawing.length - 1); i >= 0; i--) {
			printError("i : "+i);
			if(this.drawing[i].a != 3) continue;
			for(var n = 0; n < i; n++) {
				printError("n : "+n);
				if(this.drawing[n].a != 3) continue;
				if(this.shapeContains(i, this.drawing[i], n, this.drawing[n])) {
					this.drawing.splice(n, 1);
					i--;
				}	
			}
		}
	},

	shapeContains: function(i1, nodeAbove, i2, nodeBellow) {
		//only checks for two non oblique rectanges right now
		
		var naA = nodeAbove.p[0];
		var naB = {x: nodeAbove.p[1].x, y:nodeAbove.p[0].y};
		var naC = nodeAbove.p[1];
		
		document.getElementById("errorArea").innerHTML += "point a at index " + i1 + " " + i2 + " " + naA.x + " " + naA.y + "<br>";
		document.getElementById("errorArea").innerHTML += "point a at index " + i1 + " " + i2 + " " + naB.x + " " + naB.y + "<br>";
		document.getElementById("errorArea").innerHTML += "point a at index " + i1 + " " + i2 + " " + naC.x + " " + naC.y + "<br><br>";
		
		var checkPoints = new Array(4);
		checkPoints[0] = nodeBellow.p[0];
		checkPoints[1] = {x: nodeBellow.p[1].x, y:nodeBellow.p[0].y};
		checkPoints[2] = nodeBellow.p[1];
		checkPoints[3] = {x: nodeBellow.p[0].x, y:nodeBellow.p[1].y};
		
		var validPoints = 0;
		
		for(var i = 0; i < 4; i++) {
			if(checkPoints[i].x >= naA.x && checkPoints[i].x <= naB.x && checkPoints[i].y >= naA.y && checkPoints[i].y <= naC.y) {
				validPoints++;
			}
		}

		if(validPoints == 4) return true;
		return false;
	}
});