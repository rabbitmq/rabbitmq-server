/*
	Copyright (c) 2005, 2006 Rafael Robayna

	Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

	Additional Contributions by: Morris Johns
*/

	/*
		todo:
		need to fix the error with drawing the function
		need to write tutorial for using CanvasWidget

		bugs: 
		needs to be positioned absolutly and referenced absolutly - this is an issue with how mouse events are interpreted in all browsers

		CanvasWidget is a base class that handles all event listening and triggering.  A person who wishes to write
		a widget for Canvas can easily extend CanvasWidget and the few simple methods deling with drawing the widget.

		Handles checking for the canvas element and the initalization of mouse event listeners.
		to use, the drawWidget and widgetActionPerformed functions need to be extended.
	*/

	var ColorWidget = CanvasWidget.extend({
		color_red: 0,
		color_green: 0,
		color_blue: 0,
		color_alpha: 1,
		colorString: "",
		constructor: function(canvasName, position) {
			this.inherit(canvasName, position);
		},

		drawWidget: function() {
			this.context.clearRect(0,0,255,120);
			var linGradRed = this.context.createLinearGradient(0,0,255,0);
			linGradRed.addColorStop(0, 'rgba(0,'+this.color_green+','+this.color_blue+',1)');
			linGradRed.addColorStop(1, 'rgba(255,'+this.color_green+','+this.color_blue+',1)');

			var linGradGreen = this.context.createLinearGradient(0,0,255,0);
			linGradGreen.addColorStop(0, 'rgba('+this.color_red+',0,'+this.color_blue+',1)');
			linGradGreen.addColorStop(1, 'rgba('+this.color_red+',255,'+this.color_blue+',1)');

			var linGradBlue= this.context.createLinearGradient(0,0,255,0);
			linGradBlue.addColorStop(0, 'rgba('+this.color_red+','+this.color_green+',0,1)');
			linGradBlue.addColorStop(1, 'rgba('+this.color_red+','+this.color_green+',255,1)');

			var linGradAlpha= this.context.createLinearGradient(0,0,255,0);
			linGradAlpha.addColorStop(0, 'rgba('+this.color_red+','+this.color_green+','+this.color_blue+',1)');
			linGradAlpha.addColorStop(1, 'rgba('+this.color_red+','+this.color_green+','+this.color_blue+',0)');

			this.context.fillStyle = linGradRed;
			this.context.fillRect(0,0,255,20);
			this.drawColorWidgetPointer(this.color_red, 20, this.context);

			this.context.fillStyle = linGradGreen;
			this.context.fillRect(0,20,255,20);
			this.drawColorWidgetPointer(this.color_green, 40, this.context);

			this.context.fillStyle = linGradBlue;
			this.context.fillRect(0,40,255,20);
			this.drawColorWidgetPointer(this.color_blue, 60, this.context);

			this.context.fillStyle = linGradAlpha;
			this.context.fillRect(0,60,255,20);
			var alphaPosition = Math.floor((1-this.color_alpha)*255);
			this.drawColorWidgetPointer(alphaPosition, 80, this.context);

			this.context.fillStyle = "black";
			this.context.fillRect(255, 0, 275, 40);

			this.context.fillStyle = "white";
			this.context.fillRect(255, 40, 275, 40);
		},	
			
		drawColorWidgetPointer: function(xPos, yPos) {
			this.context.fillStyle = "white";
			this.context.beginPath();
			this.context.moveTo(xPos - 6, yPos);
			this.context.lineTo(xPos, yPos - 5);
			this.context.lineTo(xPos + 6, yPos);
			this.context.fill();
			
			this.context.strokeWidth = 1;
			this.context.fillStyle = "black";

			this.context.beginPath();
			this.context.arc(xPos, yPos-7.5, 2.5,0,Math.PI*2 ,true);
			this.context.fill();
			this.context.closePath();
		},
		
		checkWidgetEvent: function(e) {
			var mousePos = this.getCanvasMousePos(e);

			if(mousePos.x > 255) {
				if(mousePos.y > 0 && mousePos.y <= 40) {
					this.color_red = 0;
					this.color_green = 0;
					this.color_blue = 0;
				} else {
					this.color_red = 255;
					this.color_green = 255;
					this.color_blue = 255;
				}
			} else {
				if(mousePos.y > 0 && mousePos.y <= 20) {
					this.color_red = mousePos.x;
				} else if(mousePos.y > 20 && mousePos.y <= 40) {
					this.color_green = mousePos.x;
				} else if(mousePos.y > 40 && mousePos.y <= 60) {
					this.color_blue = mousePos.x;
				} else {
					this.color_alpha = 1 - mousePos.x/255;
				}
			}
			
			this.colorString = 'rgba('+this.color_red+','+this.color_green+','+this.color_blue+','+this.color_alpha+')';
			this.drawWidget();
			this.callWidgetListeners();
		}
	});

	var LineWidthWidget = CanvasWidget.extend({
		lineWidth: null,
		
		constructor: function(canvasName, lineWidth, position) {
			this.lineWidth = lineWidth;
			this.inherit(canvasName, position);
		},

		drawWidget: function() {
			this.context.clearRect(0,0,275,120);

			this.context.fillStyle = 'rgba(0,0,0,0.2)';
			this.context.fillRect(0, 0, 275, 76);

			this.context.strokeStyle = 'rgba(255,255,255,1)';
			this.context.moveTo(1, 38);
			this.context.lineTo(274, 38);
			this.context.stroke();

			this.context.strokeStyle = 'rgba(255,255,255,0.5)';
			this.context.moveTo(1, 19);
			this.context.lineTo(274, 19);
			this.context.moveTo(1, 57);
			this.context.lineTo(274, 57);
			this.context.stroke();
			
			this.context.beginPath();
			var linePosition = Math.floor((this.lineWidth*255)/76);
			this.context.fillStyle = 'rgba(0,0,0,1)';
			this.context.arc(linePosition, 38, this.lineWidth/2, 0, Math.PI*2, true);
			this.context.fill();
			this.context.closePath();
		},
	
		checkWidgetEvent: function(e) {
			var mousePos = this.getCanvasMousePos(e);

			if(mousePos.x >= 0 && mousePos.x <= 255) {
				this.lineWidth = Math.floor(((mousePos.x)*76)/255) + 1;
				this.drawWidget();
				this.callWidgetListeners();
			}
		}
	});

	var TransportWidget = CanvasWidget.extend({
		timeline_shift: 5,
		timeline_position: 5,
		buttons: null,
		lastTransportButton: null,
		btnSize: {x: 30, y: 24},
		btnShift: 32,
		btnSpace: 4,
		btnY: 20,
		animator: null,

		constructor: function(canvasName, position, animator) {
			this.animator = animator;
			this.inherit(canvasName, position);
			this.animator.addAnimationEventListener(this.animationEventOcccured.bindAsEventListener(this));
			this.animator.addTimeChangeEventListener(this.setTransportPosition.bindAsEventListener(this));
		},

		drawWidget: function(lineWidth) {
			this.context.clearRect(0,0,255,120);
			this.context.lineWidth = 1;
			this.context.fillStyle = "#EEEEEE";
			this.context.fillRect(0, 0, 274, 50);
			
			this.timeline_position = 5;
			this.drawTransportPosition();

			this.buttons = new Array();
			
			this.context.strokeStyle = "black";
			this.context.fillStyle = "#DDDDDD";
			this.context.lineWidth = 1;

			for(i = 1; i <= 6; i++) {
				this.drawButtonBorder(i);
			}
			this.context.fill();

			this.drawRecordButton(true);
			this.drawPlayButton(false);
			this.drawStopButton(false);
			this.drawRewindButton(false);
			this.drawForwardStepButton(false);
			this.drawForwardEndButton(false);
			
			this.lastTransportButton = 1;
			
			return transportWidget;
		},

		setButtonColor: function(action) {
			if(action) this.context.fillStyle = "red";
			else this.context.fillStyle = "black";
		},

		drawRecordButton: function(action) {
			this.context.beginPath();
			this.setButtonColor(action);
			this.context.arc(this.buttons[1] + this.btnSize.x/2, this.btnY + this.btnSize.y/2, 8, 0, Math.PI*2, true)
			this.context.fill();
			this.context.closePath();
		},
		
		drawPlayButton: function(action) {
			this.context.beginPath();
			this.setButtonColor(action);
			this.context.moveTo(this.buttons[2] + 6, this.btnY + 4);
			this.context.lineTo(this.buttons[2] + 6, this.btnY + this.btnSize.y - 4 );
			this.context.lineTo(this.btnSize.x + this.buttons[2] - 6, this.btnSize.y/2 + this.btnY);
			this.context.lineTo(this.buttons[2] + 6, this.btnY + 4);
			this.context.fill();
			this.context.closePath();
		},

		drawStopButton: function(action) {
			this.context.beginPath();
			this.setButtonColor(action);
			this.context.fillRect(this.buttons[3] + this.btnSize.x/2 - 8, this.btnY + this.btnSize.y/2 - 8, 16, 16);
			this.context.closePath();
		},

		drawRewindButton: function(action) {
			this.context.beginPath();
			this.setButtonColor(action);
			this.context.fillRect(this.buttons[4] + this.btnSize.x/2 - 7, this.btnY + 4, 3, this.btnSize.y - 8);
			this.context.moveTo(this.btnSize.x/2 + this.buttons[4] - 4, this.btnSize.y/2 + this.btnY);
			this.context.lineTo(this.buttons[4] + this.btnSize.x - 8, this.btnY + 4 );
			this.context.lineTo(this.buttons[4] + this.btnSize.x - 8, this.btnSize.y + this.btnY - 4);
			this.context.lineTo(this.btnSize.x/2 + this.buttons[4] - 4, this.btnSize.y/2 + this.btnY);
			this.context.fill();
			this.context.closePath();
		},

		drawForwardStepButton: function(action) {
			this.context.beginPath();
			this.setButtonColor(action);
			this.context.moveTo(this.buttons[5] + 4, this.btnY + 4);
			this.context.lineTo(this.buttons[5] + 4, this.btnY + this.btnSize.y - 4 );
			this.context.lineTo(this.btnSize.x/2 + this.buttons[5], this.btnSize.y/2 + this.btnY);
			this.context.lineTo(this.buttons[5] + 4, this.btnY + 4);
			this.context.moveTo(this.btnSize.x/2 + this.buttons[5], this.btnY + 4);
			this.context.lineTo(this.buttons[5] + this.btnSize.x/2, this.btnY + this.btnSize.y - 4 );
			this.context.lineTo(this.btnSize.x + this.buttons[5] - 4, this.btnSize.y/2 + this.btnY);
			this.context.lineTo(this.btnSize.x/2 + this.buttons[5], this.btnY + 4);
			this.context.fill();
			this.context.closePath();
		},

		drawForwardEndButton: function(action) {
			this.context.beginPath();
			this.setButtonColor(action);
			this.context.fillRect(this.buttons[6] + this.btnSize.x/2 + 4, this.btnY + 4, 3, this.btnSize.y - 8);
			this.context.moveTo(this.buttons[6] + 8, this.btnY + 4);
			this.context.lineTo(this.buttons[6] + 8, this.btnY + this.btnSize.y - 4 );
			this.context.lineTo(this.btnSize.x/2 + this.buttons[6] + 4, this.btnSize.y/2 + this.btnY);
			this.context.lineTo(this.buttons[6] + 8, this.btnY + 4);
			this.context.fill();
			this.context.closePath();
		},

		drawButtonBorder: function(buttonNum) {
			var space = (buttonNum>1)?this.btnShift+(this.btnSpace*buttonNum):this.btnShift;
			this.buttons[buttonNum] = space + (this.btnSize.x*(buttonNum - 1));
			buttonNum--;
			this.context.fillStyle = "#DDDDDD";
			this.context.fillRect(space + (this.btnSize.x*buttonNum), this.btnY, this.btnSize.x, this.btnSize.y);
			this.context.strokeRect(space + (this.btnSize.x*buttonNum), this.btnY, this.btnSize.x, this.btnSize.y);
			this.context.fillStyle = "black";
			this.context.fillRect(space + (this.btnSize.x*buttonNum), this.btnY - 1, this.btnSize.x, 1);
			this.context.fillRect(space + (this.btnSize.x*buttonNum) - 1, this.btnY, 1, this.btnSize.y);
		},

		//can remove and replace this with something array
		setButtonLook: function(index, action) {
			switch (index){
				case 1:
					this.drawRecordButton(action);
					break;
				case 2:
					this.drawPlayButton(action);
					break;
				case 3:
					this.drawStopButton(action);
					break;
				case 4:
					this.drawRewindButton(action);
					break;
				case 5:
					this.drawForwardStepButton(action);
					break;
				case 6:
					this.drawForwardEndButton(action);
					break;
			}
		},

		drawTransportPosition: function() {
			this.context.fillStyle =  "#EEEEEE";
			this.context.fillRect(this.timeline_shift, 7, 265, 5);
			this.context.strokeStyle = "#666666";
			this.context.strokeRect(this.timeline_shift, 8, 265, 3);
			this.context.fillStyle =  "white";
			this.context.fillRect(this.timeline_shift, 9, 265, 1);
			this.context.fillStyle = "#AAAAAA";
			this.context.fillRect(this.timeline_position, 7, 6, 4);
			this.context.fillStyle = "#000000";
		},
	
		checkWidgetEvent: function(e) {
			if(this.animator.animation == null) return;

			var mousePos = this.getCanvasMousePos(e);

			if(mousePos.y < 20 && !this.animator.isEmpty()) {
				this.timeline_position = mousePos.x;
				this.animator.goToAnimationNode(Math.floor((this.animator.animation.last.t*this.timeline_position)/255));
				this.drawTransportPosition();
			} else if(e.type != "mousemove") {
				if(mousePos.x > this.buttons[1] && mousePos.x < this.buttons[2]) {
					this.animator.startRecordAnimation();
					this.setActiveTransportButton(1);
				} else if (mousePos.x > this.buttons[2] && mousePos.x < this.buttons[3]) {
					this.animator.stopAnimation();
					if(this.animator.playAnimation()) {
						this.setActiveTransportButton(2);
					}
				} else if (mousePos.x > this.buttons[3] && mousePos.x < this.buttons[4]) {
					this.animator.stopAnimation();
					this.setActiveTransportButton(3); //not sure about removing this
				} else if (mousePos.x > this.buttons[4] && mousePos.x < this.buttons[5]) {
					this.animator.stopAnimation();
					if(!this.animator.isEmpty()) {	
						this.animator.goToAnimationNode(0);
						this.setTransportPosition();
					}
				} else if (mousePos.x > this.buttons[5] && mousePos.x < this.buttons[6]) {
					this.animator.stopAnimation();
					if(!this.animator.isEmpty()) {
						this.animator.goToAnimationNode(this.animator.curAnimationNode.n.t);
						this.setTransportPosition();
					}
					this.setActiveTransportButton(3);
				} else if (mousePos.x > this.buttons[6] && mousePos.x < (this.buttons[6] + 30)) {
					this.animator.stopAnimation();
					if(!this.animator.isEmpty()) {
						this.setActiveTransportButton(6);
						this.animator.goToAnimationNode(this.animator.animation.last.t);
						this.setTransportPosition();
						this.setActiveTransportButton(3);
					}
				}
			}
		},

		setActiveTransportButton: function(button) {
			this.drawButtonBorder(button);
			this.setButtonLook(this.lastTransportButton, false);
			this.setButtonLook(button, true);
			this.lastTransportButton = button;
		},
		
		animationEventOcccured: function(action) {
			this.setActiveTransportButton(action);
		},

		setTransportPosition: function(e) {
			var animationLength = this.animator.animation.last.t;
			var time = this.animator.curAnimationNode.t;
			this.timeline_position = Math.floor((time*260)/animationLength) + 5;
			this.drawTransportPosition();
		}
	});