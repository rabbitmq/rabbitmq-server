/*
	Copyright (c) 2005, 2006 Rafael Robayna

	Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

	Additional Contributions by: Morris Johns
*/

var CPAnimator = Base.extend({
	RECORD: 0,
	PLAY: 1,
	STOP: 2,
	animationControlState: null,
	animationClockStart: 0,
	animation: null,
	curAnimationNode: null,
	animationEventListeners: new Array(), 
	timeChangeEventListeners: new Array(),
	canvasPainter: null,
	
	constructor: function(canvasPainter) {
		this.canvasPainter = canvasPainter;
		this.newAnimation();
		this.canvasPainter.addWidgetListener(this.recordAction.bindAsEventListener(this)); 
	},
	
	newAnimation: function() {
		this.stopAnimation();
		this.animation = null;
		this.startRecordAnimation();

		var curDrawAction = canvasPainter.curDrawAction;
		this.canvasPainter.curDrawAction = 5;
		var firstNode = this.getAction();
		this.canvasPainter.curDrawAction = curDrawAction;
		this.animation.first = firstNode;
		this.animation.last = firstNode;
		this.curAnimationNode = firstNode;

		this.triggerAnimationEvent(1);
	},

	startRecordAnimation: function() {
		this.animationControlState = this.RECORD;
		if(this.animation == null || this.curAnimationNode == null) {
			this.canvasPainter.clearCanvas(canvasPainter.context);
			this.animation = new Object();
			this.curAnimationNode = null;
			this.animationClockStart = new Date().getTime();
		} else {
			this.animationClockStart = new Date().getTime() + this.curAnimationNode.time;
			this.playAnimation();
		}
	},

	recordAction: function() {
		if(this.animationControlState != this.RECORD) return;
		var animationNode = this.getAction();
		this.addNode(animationNode);
	},

	getAction: function() {
		var animationNode = new Object()
		animationNode.p = new Array(2);  //points array
		animationNode.p[0] = this.canvasPainter.startPos;
		animationNode.p[1] = this.canvasPainter.curPos;
		animationNode.a = this.canvasPainter.curDrawAction; //action
		animationNode.t = this.getAnimationTime(); //time
		animationNode.c = this.canvasPainter.drawColor; //color
		animationNode.w = this.canvasPainter.context.lineWidth; //width
		animationNode.n = null; //next
		return animationNode;
	},
	
	//TODO: not sure need to check this bit
	addNode: function(animationNode) {
		if(this.animation.last == this.animation.first || animationNode.t >= this.animation.last.t) {
			this.animation.last.n = animationNode;
			this.animation.last = animationNode;
		} else {
			if(this.curAnimationNode.n == null) return;
			while(this.curAnimationNode.n.t <= animationNode.t) {
				this.curAnimationNode = this.curAnimationNode.n;
				this.paintAnimationNode();
			}
			animationNode.n = this.curAnimationNode.n;
			this.curAnimationNode.n = animationNode;
		}
		if(this.animation.first.n == animationNode) {
			this.animationClockStart = new Date().getTime();
			this.animation.first.t = 1;
			animationNode.t = 1;
		}
		this.curAnimationNode = animationNode;
	},

	getAnimationTime: function() {
		return new Date().getTime() - this.animationClockStart;
	},

	stopAnimation: function() {
		if(this.animationControlState != this.STOP) {
			this.animationControlState = this.STOP;
			this.triggerAnimationEvent(3);
		}
	},

	playAnimation: function() {
		if(this.animation.first == this.animation.last) return false;
		
		if(this.animationControlState != this.RECORD)
			this.animationControlState = this.PLAY;

		if(this.curAnimationNode != null) {
			if(this.curAnimationNode == this.animation.last && this.animationControlState != this.RECORD) {
				this.curAnimationNode = this.animation.first;
				this.canvasPainter.clearCanvas(canvasPainter.context);
			}
			this.animationClockStart = new Date().getTime() - this.curAnimationNode.t;
			this.getAnimationNode();
		} else {
			this.animationClockStart = new Date().getTime();
			this.curAnimationNode = this.animation.first;
			this.getAnimationNode();
		}
		return true;
	},

	getAnimationNode: function() {
		if(this.animationControlState == this.STOP) return;

		if(this.curAnimationNode != this.animation.last) {
			while(this.curAnimationNode != this.animation.last && this.curAnimationNode.n.t < this.getAnimationTime()) {
				this.curAnimationNode = this.curAnimationNode.n;
				this.paintAnimationNode();
			}
			this.triggerTimeChangeEvent();
			window.setTimeout(this.getAnimationNode.bindAsEventListener(this), 5);
		}	else if(this.animationControlState != this.RECORD) {
			 this.stopAnimation();
		}
	},

	paintAnimationNode:function() {
		this.canvasPainter.context.save();
		this.canvasPainter.context.fillStyle = this.curAnimationNode.c;
		this.canvasPainter.context.strokeStyle = this.curAnimationNode.c;
		this.canvasPainter.context.lineWidth = this.curAnimationNode.w;
		this.canvasPainter.drawActions[this.curAnimationNode.a](this.curAnimationNode.p[0], this.curAnimationNode.p[1], canvasPainter.context, false);
		this.canvasPainter.context.restore();
	},

	goToAnimationNode: function(time) {
		this.stopAnimation();
		if(this.curAnimationNode.t > time) {
			this.canvasPainter.clearCanvas(canvasPainter.context);
			this.curAnimationNode = this.animation.first;
		}
		while(this.curAnimationNode != null) {
			this.paintAnimationNode();
			if(this.curAnimationNode.n == null || this.curAnimationNode.n.t > time) {
				break;
			} else {
				this.curAnimationNode = this.curAnimationNode.n;
			}
		}
	},

	isEmpty: function() {
		return this.animation.first == this.animation.last;
	},

	// Event Handlers
	triggerAnimationEvent: function(action) {
		if(this.animationEventListeners.length != 0) {
			for(var i=0; i < this.animationEventListeners.length; i++) {
				this.animationEventListeners[i](action);
			}
		}
	},

	addAnimationEventListener: function(eventListener) {
		this.animationEventListeners[this.animationEventListeners.length] = eventListener;
	},

	triggerTimeChangeEvent: function() {
		if(this.timeChangeEventListeners.length != 0) {
			for(var i=0; i < this.timeChangeEventListeners.length; i++) 
				this.timeChangeEventListeners[i](); 
		}
	},

	addTimeChangeEventListener: function(eventListener) {
		this.timeChangeEventListeners[this.timeChangeEventListeners.length] = eventListener;
	},

	saveAnimation: function() {
		return this.animation.toSource();
	}
});